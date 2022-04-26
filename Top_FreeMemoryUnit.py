from Topology import *
from FMU_CircularBuffer import *





class TopFreeMemoryUnit(Topology):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopFreeMemoryUnit, self).__init__(nRanks, configfile);
        self.nFMUs = configfile.number_of_FMUs;
        self.independent_send_recv = True;
        assert self.nFMUs > 0, "Number of Free Memory Units needs to be at least 1 when using FMUs topology"
        #assert self.eager_protocol_max_size == 0, "Eager Protocol can not be activated with this Topology"

        self.fmu_interleave = 0;
        self.fmu_last_cycle_vector = [0] * self.nFMUs;

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = FMU_CircularBuffer(self.nFMUs);

        #Override
        self.interLatency = configfile.fmu_latency;
        self.interBandwidth = configfile.fmu_bandwidth;


        self.isThereConflict = None;
        self.chooseFMU = None;
        if configfile.fmu_contention_model == "STATIC":
            self.isThereConflict = self.isThereConflict_Static;
            self.chooseFMU = self.chooseFMU_static;
        elif configfile.fmu_contention_model == "NO_CONFLICT":
            self.isThereConflict = self.isThereConflict_NoConflict;
            self.chooseFMU = self.chooseFMU_oracle;
        elif configfile.fmu_contention_model == "INTERLEAVE":
            self.isThereConflict = self.isThereConflict_Interleave;
            self.chooseFMU = self.chooseFMU_interleave;
        else:
            print( bcolors.FAIL + "ERROR: Unknown fmu contention model:  " + configfile.fmu_contention_model + bcolors.ENDC);
            sys.exit(1);


    def chooseFMU_static(self, rank):
        return rank % self.nFMUs;
    
    def chooseFMU_interleave(self, rank):
        self.fmu_interleave = (self.fmu_interleave + 1) % self.nFMUs;
        return self.fmu_interleave;

    def chooseFMU_oracle(self, rank):
        self.fmu_interleave = self.fmu_interleave + 1;
        return self.fmu_interleave;


    #def isThereConflict(self, my_rank: int, partner_rank: int, fmu_in_usage_1 = None, fmu_in_usage_2= None) -> bool:
    #    if my_rank == partner_rank:
    #        return True;

        
    def isThereConflict_NoConflict(self, my_rank: int, partner_rank: int, fmu_in_usage_1 = None, fmu_in_usage_2= None) -> bool:
        if my_rank == partner_rank:
            return True;
        return False;

    def isThereConflict_Static(self, my_rank: int, partner_rank: int, fmu_in_usage_1 = None, fmu_in_usage_2= None) -> bool:
        if my_rank == partner_rank:
            return True;
        if my_rank % self.nFMUs == partner_rank % self.nFMUs:
            return True;
        return False;

    def isThereConflict_Interleave(self, my_rank: int, partner_rank: int, fmu_in_usage_1 = None, fmu_in_usage_2= None) -> bool:
        if my_rank == partner_rank:
            return True;
        if fmu_in_usage_1 == fmu_in_usage_2:
            return True;
        return False;



    def processContention(self, matchQ, col_matchQ, currentPosition) -> MQ_Match:

        # We separate the several matches
        valid_matchesQ : list[MQ_Match]; # For valid matches
        valid_matchesQ = []
        invalid_matchesQ : list[MQ_Match]; # For invalid matches
        invalid_matchesQ = []

        ###[1] Find the valid matches
        # Valid matches are the ones that:
        #       1) match their position on the "currentPosition" tracker of the messagequeue 
        #       OR
        #       2) the ones that are untrackable (negative tag)
        # We separate the matches on two arrays, onde for the valid ones (valid_matchesQ)
        # and another for the invalid ondes (invalid_matchesQ)
        for i in range(0, len(matchQ)):
            thisMatch : MQ_Match = matchQ[i];
            
            if (
                (    
                    (thisMatch.positionS == currentPosition[thisMatch.rankS] or thisMatch.positionS < 0) and 
                    (thisMatch.positionR == currentPosition[thisMatch.rankR] or thisMatch.positionR < 0)
                ) or
                (thisMatch.tag < 0)
            ):
                valid_matchesQ.append(thisMatch)
            else:
                invalid_matchesQ.append(thisMatch);
        
        # Valid among Collectives
        for i in range(0, len(col_matchQ)):
            tmp_valid, tmp_invalid = col_matchQ[i].getValidAndInvalidMatches();
            valid_matchesQ = valid_matchesQ + tmp_valid;
            invalid_matchesQ = invalid_matchesQ + tmp_invalid;

        # We might be on a deadlock if there is no valid match on this point
        assert len(valid_matchesQ) > 0, "No valid Match was found"

        # *******************************************************************************************************************************


        # Check for not initialized matches, and initialize them
        for i in range(len(valid_matchesQ)):
            if not valid_matchesQ[i].initialized:
                #valid_matchesQ[i].sep_initializeMatch(self.SimpleCommunicationCalculusInternode(valid_matchesQ[i].size));
                valid_matchesQ[i].sep_initializeMatch(self.CommunicationCalculus_Bandwidth(valid_matchesQ[i].rankS, valid_matchesQ[i].rankR, valid_matchesQ[i].size)[0]);
                #chosenFMU = self.fmu_interleave % self.nFMUs;
                chosenFMU = self.chooseFMU(valid_matchesQ[i].rankR);
                valid_matchesQ[i].fmu_in_use = chosenFMU;
                if chosenFMU < self.nFMUs:
                    minToStart = self.fmu_last_cycle_vector[chosenFMU] + valid_matchesQ[i].latency;
                    inc = minToStart - valid_matchesQ[i].sep_getBaseCycle();
                    if inc > 0:
                        valid_matchesQ[i].sep_incrementCycle(inc);
                

        # find lowest cycle
        readyMatch : MQ_Match
        readyMatch = None;

        while readyMatch == None:

            lowest_cycle = valid_matchesQ[0].sep_getBaseCycle();
            li = 0;
            for i in range(0, len(valid_matchesQ)):
                if valid_matchesQ[i].sep_getBaseCycle() < lowest_cycle:
                    lowest_cycle = valid_matchesQ[i].sep_getBaseCycle();
                    li = i;

            readyMatch = valid_matchesQ[li];

            rank_in_usage = None;
            if readyMatch.still_solving_send:
                rank_in_usage = readyMatch.rankS;
            else:
                rank_in_usage = readyMatch.rankR;
            
            # Delay other communications as needed
            for j in range(0, len(valid_matchesQ)):
                if j == li:
                    continue;
                
                
                partner_rank = None;
                if valid_matchesQ[j].still_solving_send:
                    partner_rank = valid_matchesQ[j].rankS;
                else:
                    partner_rank = valid_matchesQ[j].rankR;

                if (
                    self.isThereConflict(rank_in_usage, partner_rank, readyMatch.fmu_in_use, valid_matchesQ[j].fmu_in_use)
                ):
                    minToStart = readyMatch.sep_getEndCycle() + valid_matchesQ[j].latency;
                    inc = minToStart - valid_matchesQ[j].sep_getBaseCycle();

                    if inc > 0:
                        valid_matchesQ[j].sep_incrementCycle(inc);

            for j in range(0, len(invalid_matchesQ)):
                
                partner_rank = None;
                if invalid_matchesQ[j].still_solving_send:
                    partner_rank = invalid_matchesQ[j].rankS;
                else:
                    partner_rank = invalid_matchesQ[j].rankR;
                

                # Only check if ranks are equal.
                # As invalid matches still do not have an assigned FMU, 
                # We treat delays due FMU on initialization using fmu_last_cycle_vector
                if (
                               (rank_in_usage == partner_rank) 
                ):
                    minToStart = readyMatch.sep_getEndCycle() + invalid_matchesQ[j].latency;
                    inc = minToStart - invalid_matchesQ[j].sep_getBaseCycle();

                    if inc > 0:
                        invalid_matchesQ[j].sep_incrementCycle(inc);


            if readyMatch.still_solving_send:
                readyMatch.sep_move_RECV_after_SEND();
                
                #self.fmu_circularBuffer.insert_entry(readyMatch.rankR % self.nFMUs,
                #                                     readyMatch.id,
                #                                     readyMatch.size);

                self.fmu_circularBuffer.insert_entry(readyMatch.fmu_in_use,
                                                     readyMatch.id,
                                                     readyMatch.size);
                
                readyMatch = None;



        
        #self.fmu_circularBuffer.consume_entry(readyMatch.rankR % self.nFMUs,
        #                                      readyMatch.id);
        
        self.fmu_circularBuffer.consume_entry(readyMatch.fmu_in_use,
                                              readyMatch.id);

        
        if readyMatch.fmu_in_use < self.nFMUs:
            assert readyMatch.endCycle >= self.fmu_last_cycle_vector[readyMatch.fmu_in_use], "what? " + str(readyMatch.endCycle) + " < " + str(self.fmu_last_cycle_vector[readyMatch.fmu_in_use])
            self.fmu_last_cycle_vector[readyMatch.fmu_in_use] = readyMatch.endCycle;
        


        readyMatchID = readyMatch.id;
        # Grab the ready match from the matches queue (matchQ) or collectives matches queue (col_matchQ)
        readyMatch = None;
        for j in range(0, len(matchQ)):
            if readyMatchID == matchQ[j].id:
               readyMatch = matchQ.pop(j)
               break;
        if readyMatch is None:
            for j in range(0, len(col_matchQ)):
                readyMatch = col_matchQ[j].getMatchByID(readyMatchID);
                if readyMatch is not None:
                    break;
        # If readyMatch is None, it does not exist... what happened?        
        assert readyMatch is not None, "ready match is not presented on matches queues"


        #print("Processing contention complete.")
        return readyMatch;


        
        