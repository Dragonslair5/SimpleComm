from Topology import *
from FMU_CircularBuffer import *





class TopFreeMemoryIndependent(Topology):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopFreeMemoryIndependent, self).__init__(nRanks, configfile);
        self.nFMUs = configfile.number_of_FMUs;
        self.independent_send_recv = True;
        assert self.nFMUs > 0, "Number of Free Memory Units needs to be at least 1 when using FMUs topology"
        #assert self.eager_protocol_max_size == 0, "Eager Protocol can not be activated with this Topology"

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = FMU_CircularBuffer(self.nFMUs);

        #Override
        self.interLatency = configfile.fmu_latency;
        self.interBandwidth = configfile.fmu_bandwidth;




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

        print("\n***")
        for i in range(len(valid_matchesQ)):
            #print( str(valid_matchesQ[i].sep_getBaseCycle()) + " " + str(valid_matchesQ[i].endCycle) + " fmu: " + str(valid_matchesQ[i].fmu_in_use))
            print(str(valid_matchesQ[i].id) + " " + str(valid_matchesQ[i].sep_getBaseCycle()))
        print("***")

        # Check for not initialized matches, and initialize them
        for i in range(len(valid_matchesQ)):
            if not valid_matchesQ[i].initialized:
                #valid_matchesQ[i].sep_initializeMatch(self.SimpleCommunicationCalculusInternode(valid_matchesQ[i].size));
                valid_matchesQ[i].sep_initializeMatch(self.CommunicationCalculus_Bandwidth(valid_matchesQ[i].rankS, valid_matchesQ[i].rankR, valid_matchesQ[i].size)[0]);
                print(str(valid_matchesQ[i].latency))


        print("\n***")
        for i in range(len(valid_matchesQ)):
            #print( str(valid_matchesQ[i].sep_getBaseCycle()) + " " + str(valid_matchesQ[i].endCycle) + " fmu: " + str(valid_matchesQ[i].fmu_in_use))
            print(str(valid_matchesQ[i].id) + " " + str(valid_matchesQ[i].sep_getBaseCycle()))
        print("***")


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
                               (rank_in_usage == partner_rank) or
                               (readyMatch.rankR % self.nFMUs == valid_matchesQ[j].rankR % self.nFMUs)
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
                
                
                if (
                               (rank_in_usage == partner_rank) or
                               (readyMatch.rankR % self.nFMUs == invalid_matchesQ[j].rankR % self.nFMUs)
                ):
                    minToStart = readyMatch.sep_getEndCycle() + invalid_matchesQ[j].latency;
                    inc = minToStart - invalid_matchesQ[j].sep_getBaseCycle();

                    if inc > 0:
                        invalid_matchesQ[j].sep_incrementCycle(inc);


            if readyMatch.still_solving_send:
                readyMatch.sep_move_RECV_after_SEND();
                
                self.fmu_circularBuffer.insert_entry(readyMatch.rankR % self.nFMUs,
                                                     readyMatch.id,
                                                     readyMatch.size);
                
                readyMatch = None;



        
        self.fmu_circularBuffer.consume_entry(readyMatch.rankR % self.nFMUs,
                                              readyMatch.id);
        

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

        print("S2 " + str(readyMatch.endCycle))
        #print("Processing contention complete.")
        return readyMatch;


        
        