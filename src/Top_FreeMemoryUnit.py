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
        self.fmu_congestion_time = [0] * self.nFMUs;

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = FMU_CircularBuffer(self.nFMUs);

        #Override
        self.interLatency = configfile.fmu_latency;
        self.interBandwidth = configfile.fmu_bandwidth;


        self.isThereConflict = None;
        self.chooseFMU = None;
        self.update_fmu_last_cycle = None;
        self.get_fmu_last_cycle = None;
        if configfile.fmu_mapping == "STATIC":
            self.isThereConflict = self.isThereConflict_Static;
            self.chooseFMU = self.chooseFMU_static;
            # ON / ON
            self.update_fmu_last_cycle = self.update_fmu_last_cycle_TURNED_ON;
            self.get_fmu_last_cycle = self.get_fmu_last_cycle_TUNED_ON;
        elif configfile.fmu_mapping == "NO_CONFLICT":
            self.isThereConflict = self.isThereConflict_NoConflict;
            self.chooseFMU = self.chooseFMU_oracle;
            # OFF / OFF
            self.update_fmu_last_cycle = self.update_fmu_last_cycle_TURNED_OFF;
            self.get_fmu_last_cycle = self.get_fmu_last_cycle_TUNED_OFF;
        elif configfile.fmu_mapping == "INTERLEAVE":
            self.isThereConflict = self.isThereConflict_Interleave;
            self.chooseFMU = self.chooseFMU_interleave;
            # ON / ON
            self.update_fmu_last_cycle = self.update_fmu_last_cycle_TURNED_ON;
            self.get_fmu_last_cycle = self.get_fmu_last_cycle_TUNED_ON;
        else:
            print( bcolors.FAIL + "ERROR: Unknown fmu mapping scheme:  " + configfile.fmu_mapping + bcolors.ENDC);
            sys.exit(1);

        # Contention Function
        if configfile.fmu_contention_model == "SINGLE_USE":
            self.processContentionX = self.contention_singleUse;
        elif configfile.fmu_contention_model == "AS_MUCH_AS_POSSIBLE":
            self.processContentionX = self.contention_useAsMuchAsPossible;
        else:
            print( bcolors.FAIL + "ERROR: Unknown fmu contention model:  " + configfile.fmu_contention_model + bcolors.ENDC);
            sys.exit(1);

    # Choose FMU Functions
    def chooseFMU_static(self, rank):
        return rank % self.nFMUs;
    
    def chooseFMU_interleave(self, rank):
        self.fmu_interleave = (self.fmu_interleave + 1) % self.nFMUs;
        return self.fmu_interleave;

    def chooseFMU_oracle(self, rank):
        self.fmu_interleave = self.fmu_interleave + 1;
        return self.fmu_interleave;
    # *********************************************


    # FMU LAST CYCLE VECTOR Functions
    def get_fmu_last_cycle_TUNED_OFF(self, chosenFMU: int)->float:
        return 0;

    def get_fmu_last_cycle_TUNED_ON(self, chosenFMU: int)->float:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        return self.fmu_last_cycle_vector[chosenFMU];

    def update_fmu_last_cycle_TURNED_OFF(self, chosenFMU: int, endTime: float)-> None:
        pass;

    def update_fmu_last_cycle_TURNED_ON(self, chosenFMU: int, endTime: float)-> None:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        assert self.fmu_last_cycle_vector[chosenFMU] <= endTime, "what? " + str(endTime) + " < " + str(self.fmu_last_cycle_vector[chosenFMU])
        self.fmu_last_cycle_vector[chosenFMU] = endTime;
    # *********************************************
        


    # Conflict Functions        
    def isThereConflict_NoConflict(self, my_rank: int, partner_rank: int, fmu_in_usage_1 = None, fmu_in_usage_2= None) -> bool:
        if my_rank == partner_rank:
            return True;
        return False;

    def isThereConflict_Static(self, my_rank: int, partner_rank: int, fmu_in_usage_1 = None, fmu_in_usage_2= None) -> bool:
        if my_rank == partner_rank:
            return True;
        if fmu_in_usage_1 == fmu_in_usage_2:
            return True;
        #if my_rank % self.nFMUs == partner_rank % self.nFMUs:
        #    return True;
        return False;

    def isThereConflict_Interleave(self, my_rank: int, partner_rank: int, fmu_in_usage_1 = None, fmu_in_usage_2= None) -> bool:
        if my_rank == partner_rank:
            return True;
        if fmu_in_usage_1 == fmu_in_usage_2:
            return True;
        return False;
    # *********************************************




    def initializeUnitializedMatches(self, valid_matchesQ: typing.List[MQ_Match])-> None:
        for i in range(len(valid_matchesQ)):
            if not valid_matchesQ[i].initialized:
                #valid_matchesQ[i].sep_initializeMatch(self.SimpleCommunicationCalculusInternode(valid_matchesQ[i].size));
                valid_matchesQ[i].sep_initializeMatch(self.CommunicationCalculus_Bandwidth(valid_matchesQ[i].rankS, valid_matchesQ[i].rankR, valid_matchesQ[i].size)[0]);
                #chosenFMU = self.fmu_interleave % self.nFMUs;
                chosenFMU = self.chooseFMU(valid_matchesQ[i].rankR);
                valid_matchesQ[i].fmu_in_use = chosenFMU;
                if chosenFMU < self.nFMUs:
                    #minToStart = self.fmu_last_cycle_vector[chosenFMU] + valid_matchesQ[i].latency;
                    minToStart = self.get_fmu_last_cycle(chosenFMU) + valid_matchesQ[i].latency;
                    #print("fmuLast: " + str(self.fmu_last_cycle_vector[chosenFMU]) + " Lat: " + str(valid_matchesQ[i].latency))
                    #print(str(minToStart) + " [] " + str(valid_matchesQ[i].sep_getBaseCycle()))
                    inc = minToStart - valid_matchesQ[i].sep_getBaseCycle();
                    if inc > 0:
                        valid_matchesQ[i].sep_incrementCycle(inc);
                        self.fmu_congestion_time[chosenFMU] = self.fmu_congestion_time[chosenFMU] + inc;



    def processContention(self, matchQ) -> MQ_Match:
        return self.processContentionX(matchQ)


# ****************************************************************
#   ____ ___  _   _ _____ _____ _   _ _____ ___ ___  _   _ 
#  / ___/ _ \| \ | |_   _| ____| \ | |_   _|_ _/ _ \| \ | |
# | |  | | | |  \| | | | |  _| |  \| | | |  | | | | |  \| |
# | |__| |_| | |\  | | | | |___| |\  | | |  | | |_| | |\  |
#  \____\___/|_| \_| |_| |_____|_| \_| |_| |___\___/|_| \_|
#                                                          
#  _____ _   _ _   _  ____ _____ ___ ___  _   _ ____  
# |  ___| | | | \ | |/ ___|_   _|_ _/ _ \| \ | / ___| 
# | |_  | | | |  \| | |     | |  | | | | |  \| \___ \ 
# |  _| | |_| | |\  | |___  | |  | | |_| | |\  |___) |
# |_|    \___/|_| \_|\____| |_| |___\___/|_| \_|____/ 
#                                                     
# ****************************************************************

    def contention_singleUse(self, matchQ) -> MQ_Match:

        valid_matchesQ = matchQ;

        #print("Valid: " + str(len(valid_matchesQ)) + " Invalid: " + str(len(invalid_matchesQ)))
        # We might be on a deadlock if there is no valid match on this point
        assert len(valid_matchesQ) > 0, "No valid Match was found"

        # Check for not initialized matches, and initialize them
        self.initializeUnitializedMatches(valid_matchesQ);

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
            #print("\nreadyMatch = " + str(readyMatch.id))

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
                        if readyMatch.fmu_in_use == valid_matchesQ[j].fmu_in_use:
                            self.fmu_congestion_time[readyMatch.fmu_in_use] = self.fmu_congestion_time[readyMatch.fmu_in_use] + inc;

            if readyMatch.still_solving_send:
                readyMatch.sep_move_RECV_after_SEND();
                
                #self.fmu_circularBuffer.insert_entry(readyMatch.rankR % self.nFMUs,
                #                                     readyMatch.id,
                #                                     readyMatch.size);

                self.fmu_circularBuffer.insert_entry(readyMatch.fmu_in_use,
                                                     readyMatch.id,
                                                     readyMatch.size);
                
                readyMatch = None;

        self.fmu_circularBuffer.consume_entry(readyMatch.fmu_in_use,
                                              readyMatch.id);


        assert readyMatch.endCycle == readyMatch.recv_endCycle, "Why are they not equal? " + str(readyMatch.endCycle) + " != " + str(readyMatch.recv_endCycle)
        self.update_fmu_last_cycle(readyMatch.fmu_in_use, readyMatch.endCycle);
        


        readyMatchID = readyMatch.id;
        # Grab the ready match from the matches queue (matchQ) or collectives matches queue (col_matchQ)
        readyMatch = None;
        for j in range(0, len(matchQ)):
            if readyMatchID == matchQ[j].id:
               readyMatch = matchQ.pop(j)
               break;

        assert readyMatch is not None, "ready match is not presented on matches queues"


        return readyMatch;



    def contention_useAsMuchAsPossible(self, matchQ) -> MQ_Match:

        valid_matchesQ = matchQ;

        #print("Valid: " + str(len(valid_matchesQ)) + " Invalid: " + str(len(invalid_matchesQ)))
        # We might be on a deadlock if there is no valid match on this point
        assert len(valid_matchesQ) > 0, "No valid Match was found"

        # Check for not initialized matches, and initialize them
        self.initializeUnitializedMatches(valid_matchesQ);

        # find lowest cycle
        readyMatch : MQ_Match
        readyMatch = None;

        # Check if some match is ready
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].READY:
                readyMatch = valid_matchesQ[i];
                break;

        # If none is ready, lets do this
        while readyMatch == None:

            lowest_cycle = valid_matchesQ[0].sep_getBaseCycle();
            li = 0;
            for i in range(0, len(valid_matchesQ)):
                if valid_matchesQ[i].sep_getBaseCycle() < lowest_cycle:
                    lowest_cycle = valid_matchesQ[i].sep_getBaseCycle();
                    li = i;

            readyMatch = valid_matchesQ[li];
            #print("\nreadyMatch = " + str(readyMatch.id))

            rank_in_usage = None;
            if readyMatch.still_solving_send:
                rank_in_usage = readyMatch.rankS;
            else:
                rank_in_usage = readyMatch.rankR;
            
            # Delay other communications as needed
            for j in range(0, len(valid_matchesQ)):
                if j == li:
                    continue;
                
                partner: MQ_Match;
                partner = valid_matchesQ[j];


                partner_rank = None;
                if valid_matchesQ[j].still_solving_send:
                    partner_rank = valid_matchesQ[j].rankS;
                else:
                    partner_rank = valid_matchesQ[j].rankR;

                # Check if we can move something else to this FMU
                if (readyMatch.fmu_in_use == partner.fmu_in_use): # If same FMU
                    if (rank_in_usage == partner_rank): # If same Rank
                        if (partner.sep_getBaseCycle() - partner.latency) <= readyMatch.sep_getEndCycle(): # If issued before this access endCycle
                            #print("IT IS HAPPENING")
                            # Remove the switch latency
                            partner.sep_decrementCycle(partner.latency);
                            # Adjust the Timing 
                            minToStart = readyMatch.sep_getEndCycle();
                            inc = minToStart - partner.sep_getBaseCycle();
                            if inc > 0:
                                partner.sep_incrementCycle(inc);
                            
                            if partner.still_solving_send: # IF it is a SEND
                                partner.sep_move_RECV_after_SEND();
                                self.fmu_circularBuffer.insert_entry(partner.fmu_in_use,
                                                     partner.id,
                                                     partner.size);
                            else: # If it is a RECV
                                self.fmu_circularBuffer.consume_entry(partner.fmu_in_use,
                                              partner.id);
                                partner.READY = True;


                if (
                    self.isThereConflict(rank_in_usage, partner_rank, readyMatch.fmu_in_use, valid_matchesQ[j].fmu_in_use)
                ):
                    minToStart = readyMatch.sep_getEndCycle() + valid_matchesQ[j].latency;
                    inc = minToStart - valid_matchesQ[j].sep_getBaseCycle();

                    if inc > 0:
                        valid_matchesQ[j].sep_incrementCycle(inc);
                        if readyMatch.fmu_in_use == valid_matchesQ[j].fmu_in_use:
                            self.fmu_congestion_time[readyMatch.fmu_in_use] = self.fmu_congestion_time[readyMatch.fmu_in_use] + inc;

            if readyMatch.still_solving_send:
                readyMatch.sep_move_RECV_after_SEND();
                
                #self.fmu_circularBuffer.insert_entry(readyMatch.rankR % self.nFMUs,
                #                                     readyMatch.id,
                #                                     readyMatch.size);

                self.fmu_circularBuffer.insert_entry(readyMatch.fmu_in_use,
                                                     readyMatch.id,
                                                     readyMatch.size);
                
                readyMatch = None;

        if readyMatch.READY == False: # It was not solved with another operation
            self.fmu_circularBuffer.consume_entry(readyMatch.fmu_in_use,
                                              readyMatch.id);


        assert readyMatch.endCycle == readyMatch.recv_endCycle, "Why are they not equal? " + str(readyMatch.endCycle) + " != " + str(readyMatch.recv_endCycle)
        self.update_fmu_last_cycle(readyMatch.fmu_in_use, readyMatch.endCycle);
        


        readyMatchID = readyMatch.id;
        # Grab the ready match from the matches queue (matchQ) or collectives matches queue (col_matchQ)
        readyMatch = None;
        for j in range(0, len(matchQ)):
            if readyMatchID == matchQ[j].id:
               readyMatch = matchQ.pop(j)
               break;

        assert readyMatch is not None, "ready match is not presented on matches queues"


        return readyMatch;



        
        

    