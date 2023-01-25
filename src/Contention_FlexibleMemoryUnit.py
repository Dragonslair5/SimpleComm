from MPI_Constants import *
from FMU_CircularBuffer import *
import math






class Contention_FlexibleMemoryUnit:


    @staticmethod
    def getMeTheContentionMethod(nRanks: int, configfile: SimpleCommConfiguration):
        
        if configfile.fmu_mapping == "STATIC":
            return Contention_FlexibleMemoryUnit_STATIC(nRanks, configfile);
        elif configfile.fmu_mapping == "INTERLEAVE":
            return Contention_FlexibleMemoryUnit_Incremental(nRanks, configfile);
        elif configfile.fmu_mapping == "LEAST_USED_FMU":
            return Contention_FlexibleMemoryUnit_LeastUsedFMU(nRanks, configfile);
        elif configfile.fmu_mapping == "GENERAL":
            return Contention_FlexibleMemoryUnit_General(nRanks, configfile);
        else:
            print( bcolors.FAIL + "ERROR: Unknown fmu mapping scheme:  " + configfile.fmu_mapping + bcolors.ENDC);
            sys.exit(1);


    def __init__(self, nRanks: int, configfile: SimpleCommConfiguration):


        self.nRanks = nRanks;
        self.cores_per_node = 1; # TODO: Fix this after you include this option
        self.independent_send_recv = True;
        self.eager_protocol_max_size = configfile.eager_protocol_max_size;

        #Override (?)(on topology version of this, it is an override)
        self.interLatency = configfile.fmu_latency;
        self.interBandwidth = configfile.fmu_bandwidth;
        self.intraLatency = configfile.intranode_latency;
        self.intraBandwidth = configfile.intranode_bandwidth;

        self.nFMUs = configfile.number_of_FMUs;
        assert self.nFMUs > 0, "Number of Free Memory Units needs to be at least 1 when using FMUs topology"
        
        self.fmu_last_cycle_vector = [0] * self.nFMUs;
        self.fmu_request_tracker = [0] * self.nFMUs;

        self.fmu_congestion_time = [0] * self.nFMUs;
        self.channel_congestion_time = [0] * nRanks;

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = FMU_CircularBuffer(self.nFMUs);

        self.fmu_idle_mapping = 0;
        self.fmu_heuristic_mapping = 0;


# ************************************************************
#  _____ _           _               __  __           _      _ 
# |_   _(_)_ __ ___ (_)_ __   __ _  |  \/  | ___   __| | ___| |
#   | | | | '_ ` _ \| | '_ \ / _` | | |\/| |/ _ \ / _` |/ _ \ |
#   | | | | | | | | | | | | | (_| | | |  | | (_) | (_| |  __/ |
#   |_| |_|_| |_| |_|_|_| |_|\__, | |_|  |_|\___/ \__,_|\___|_|
#                            |___/                             
# ************************************************************

    
    def CommunicationCalculus_Bandwidth(self, rankS: int, rankR: int, workload: int):

        workload = int(workload) + 16; # 16 Bytes as MPI overhead (based on SimGrid)

        nodeS = rankS // self.cores_per_node;
        nodeR = rankR // self.cores_per_node;

        bandwidth: float;
        if nodeS == nodeR: # Intranode
            bandwidth=self.intraBandwidth;
        else: #Internode
            bandwidth=self.interBandwidth;

        if bandwidth == 0:
            return 0, bandwidth;
        else:
            return workload/bandwidth, bandwidth;

    def CommunicationCalculus_Latency(self, rankS: int, rankR: int, workload: int):

        if rankS == rankR:
            return 0;

        nodeS = rankS // self.cores_per_node;
        nodeR = rankR // self.cores_per_node;

        latency: float;
        if nodeS == nodeR: # Intranode
            latency = self.intraLatency;
        else: # Internode
            latency = self.interLatency;

        return latency;


# *********************************************
#  __  __                   _                 
# |  \/  | __ _ _ __  _ __ (_)_ __   __ _ ___ 
# | |\/| |/ _` | '_ \| '_ \| | '_ \ / _` / __|
# | |  | | (_| | |_) | |_) | | | | | (_| \__ \
# |_|  |_|\__,_| .__/| .__/|_|_| |_|\__, |___/
#              |_|   |_|            |___/     
# *********************************************



















# ********************************
#  ____ _____  _  _____ ___ ____ 
# / ___|_   _|/ \|_   _|_ _/ ___|
# \___ \ | | / _ \ | |  | | |    
#  ___) || |/ ___ \| |  | | |___ 
# |____/ |_/_/   \_\_| |___\____|
#                                
#********************************

class Contention_FlexibleMemoryUnit_STATIC(Contention_FlexibleMemoryUnit):


    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(Contention_FlexibleMemoryUnit_STATIC, self).__init__(nRanks, configfile);


    def get_fmu_last_cycle(self, chosenFMU: int)->float:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        return self.fmu_last_cycle_vector[chosenFMU];

    def chooseFMU(self, rank):
        return rank % self.nFMUs;

    def update_fmu_last_cycle(self, chosenFMU: int, endTime: float)-> None:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        assert self.fmu_last_cycle_vector[chosenFMU] <= endTime, "what? " + str(endTime) + " < " + str(self.fmu_last_cycle_vector[chosenFMU])
        self.fmu_last_cycle_vector[chosenFMU] = endTime;

    def isThereConflict(self, my_rank: int, partner_rank: int, fmu_in_usage_1 = None, fmu_in_usage_2= None) -> bool:
        if my_rank == partner_rank:
            return True;
        if fmu_in_usage_1 == fmu_in_usage_2:
            return True;
        return False;

    def initializeUnitializedMatches(self, valid_matchesQ: typing.List[MQ_Match])-> None:
        for i in range(len(valid_matchesQ)):
            if not valid_matchesQ[i].initialized:
                valid_matchesQ[i].sep_initializeMatch(self.CommunicationCalculus_Bandwidth(valid_matchesQ[i].rankS, valid_matchesQ[i].rankR, valid_matchesQ[i].size)[0]);
                chosenFMU = self.chooseFMU(valid_matchesQ[i].rankR);
                valid_matchesQ[i].fmu_in_use = chosenFMU;
                if chosenFMU < self.nFMUs:
                    minToStart = self.get_fmu_last_cycle(chosenFMU) + valid_matchesQ[i].latency;
                    inc = minToStart - valid_matchesQ[i].sep_getBaseCycle();
                    if inc > 0:
                        valid_matchesQ[i].sep_incrementCycle(inc);
                        self.fmu_congestion_time[chosenFMU] = self.fmu_congestion_time[chosenFMU] + inc;


    

    def processContention(self, matchQ) -> MQ_Match:
        valid_matchesQ = matchQ;

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









# ******************************************************************
#  ___ _   _  ____ ____  _____ __  __ _____ _   _ _____  _    _     
# |_ _| \ | |/ ___|  _ \| ____|  \/  | ____| \ | |_   _|/ \  | |    
#  | ||  \| | |   | |_) |  _| | |\/| |  _| |  \| | | | / _ \ | |    
#  | || |\  | |___|  _ <| |___| |  | | |___| |\  | | |/ ___ \| |___ 
# |___|_| \_|\____|_| \_\_____|_|  |_|_____|_| \_| |_/_/   \_\_____|
#                                                                   
# ******************************************************************





class Contention_FlexibleMemoryUnit_Incremental(Contention_FlexibleMemoryUnit):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(Contention_FlexibleMemoryUnit_Incremental, self).__init__(nRanks, configfile);
        self.fmu_interleave = 0;


    def get_fmu_last_cycle(self, chosenFMU: int)->float:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        return self.fmu_last_cycle_vector[chosenFMU];

    def chooseFMU(self, rank):
        self.fmu_interleave = (self.fmu_interleave + 1) % self.nFMUs;
        return self.fmu_interleave;

    def update_fmu_last_cycle(self, chosenFMU: int, endTime: float)-> None:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        assert self.fmu_last_cycle_vector[chosenFMU] <= endTime, "what? " + str(endTime) + " < " + str(self.fmu_last_cycle_vector[chosenFMU])
        self.fmu_last_cycle_vector[chosenFMU] = endTime;

    def isThereConflict(self, my_rank: int, partner_rank: int, fmu_in_usage_1 = None, fmu_in_usage_2= None) -> bool:
        if my_rank == partner_rank:
            return True;
        if fmu_in_usage_1 == fmu_in_usage_2:
            return True;
        return False;

    def initializeUnitializedMatches(self, valid_matchesQ: typing.List[MQ_Match])-> None:
        for i in range(len(valid_matchesQ)):
            if not valid_matchesQ[i].initialized:
                valid_matchesQ[i].sep_initializeMatch(self.CommunicationCalculus_Bandwidth(valid_matchesQ[i].rankS, valid_matchesQ[i].rankR, valid_matchesQ[i].size)[0]);
                chosenFMU = self.chooseFMU(valid_matchesQ[i].rankR);
                valid_matchesQ[i].fmu_in_use = chosenFMU;
                if chosenFMU < self.nFMUs:
                    minToStart = self.get_fmu_last_cycle(chosenFMU) + valid_matchesQ[i].latency;
                    inc = minToStart - valid_matchesQ[i].sep_getBaseCycle();
                    if inc > 0:
                        valid_matchesQ[i].sep_incrementCycle(inc);
                        self.fmu_congestion_time[chosenFMU] = self.fmu_congestion_time[chosenFMU] + inc;


    def processContention(self, matchQ) -> MQ_Match:
        valid_matchesQ = matchQ;

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

























# ***************************************************************
#  _     _____    _    ____ _____        _   _ ____  _____ ____  
# | |   | ____|  / \  / ___|_   _|      | | | / ___|| ____|  _ \ 
# | |   |  _|   / _ \ \___ \ | |        | | | \___ \|  _| | | | |
# | |___| |___ / ___ \ ___) || |        | |_| |___) | |___| |_| |
# |_____|_____/_/   \_\____/ |_|____ ____\___/|____/|_____|____/ 
#                             |_____|_____|                      
# ***************************************************************


class Contention_FlexibleMemoryUnit_LeastUsedFMU(Contention_FlexibleMemoryUnit):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(Contention_FlexibleMemoryUnit_LeastUsedFMU, self).__init__(nRanks, configfile);
        self.fmu_data_written_on = [0] * self.nFMUs;


    def get_fmu_last_cycle(self, chosenFMU: int)->float:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        return self.fmu_last_cycle_vector[chosenFMU];


    def update_fmu_last_cycle(self, chosenFMU: int, endTime: float)-> None:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        assert self.fmu_last_cycle_vector[chosenFMU] < endTime or math.isclose(self.fmu_last_cycle_vector[chosenFMU], endTime), "what? " + str(endTime) + " < " + str(self.fmu_last_cycle_vector[chosenFMU])
        self.fmu_last_cycle_vector[chosenFMU] = endTime;



    def processContention(self, matchQ) -> MQ_Match:

        valid_matchesQ = matchQ;

        #print("Valid: " + str(len(valid_matchesQ)) )
        # We might be on a deadlock if there is no valid match on this point
        assert len(valid_matchesQ) > 0, "No valid Match was found"

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
            #print(self.fmu_data_written_on)
            # Find earliest
            lowest_cycle = valid_matchesQ[0].sep_getBaseCycle();
            li = 0;
            for i in range(0, len(valid_matchesQ)):
                if valid_matchesQ[i].sep_getBaseCycle() < lowest_cycle:
                    lowest_cycle = valid_matchesQ[i].sep_getBaseCycle();
                    li = i;
            readyMatch = valid_matchesQ[li];

            # Initialize if not yet
            if not readyMatch.initialized:
                readyMatch.sep_initializeMatch(self.CommunicationCalculus_Bandwidth(readyMatch.rankS, readyMatch.rankR, readyMatch.size)[0]);

            fmu_in_use = None;
            # Choose FMU if is a SEND
            if readyMatch.still_solving_send:
                assert readyMatch.fmu_in_use == None;
                # Choose FMU
                # Find lowest used FMU by amount of data
                fmu_in_use = 0;
                for i in range(0,len(self.fmu_data_written_on)):
                    if self.fmu_data_written_on[i] < self.fmu_data_written_on[fmu_in_use]:
                        fmu_in_use = i;
                readyMatch.fmu_in_use = fmu_in_use;

            fmu_in_use = readyMatch.fmu_in_use;
            assert fmu_in_use is not None;

            # Adjust time based on the chosen FMU
            minToStart = self.get_fmu_last_cycle(fmu_in_use) + readyMatch.latency;
            inc = minToStart - readyMatch.sep_getBaseCycle();
            if inc > 0:
                readyMatch.sep_incrementCycle(inc);
                self.fmu_congestion_time[fmu_in_use] += inc;


            # ACTIONS
            # - Check if we can move something else to this FMU
            # - Delay other communications as needed
            for j in range(0, len(valid_matchesQ)):
                if j == li:
                    continue;
                partner: MQ_Match;
                partner = valid_matchesQ[j];

                rank_in_usage = None;
                if readyMatch.still_solving_send:
                    rank_in_usage = readyMatch.rankS;
                else:
                    rank_in_usage = readyMatch.rankR;

                partner_rank = None;
                if partner.still_solving_send:
                    partner_rank = partner.rankS;
                else:
                    partner_rank = partner.rankR;

                # Check
                if partner.fmu_in_use is not None: 
                    if rank_in_usage == partner_rank: # If same Rank
                        if readyMatch.fmu_in_use == partner.fmu_in_use: # If same FMU
                            if (partner.sep_getBaseCycle() - partner.latency) <= readyMatch.sep_getEndCycle(): # If issued before this access endCycle
                                partner.sep_decrementCycle(partner.latency);
                                minToStart = readyMatch.sep_getEndCycle();
                                inc = minToStart - partner.sep_getBaseCycle();
                                if inc > 0:
                                    partner.sep_incrementCycle(inc);

                                if partner.still_solving_send: # IF it is a SEND
                                    partner.sep_move_RECV_after_SEND();
                                    #print(str(partner.id) + " -> " + str(partner.fmu_in_use)  + "  FUSED")
                                    self.update_fmu_last_cycle(readyMatch.fmu_in_use, partner.send_endCycle);
                                    self.fmu_circularBuffer.insert_entry(partner.fmu_in_use,
                                                         partner.id,
                                                         partner.size);
                                    self.fmu_data_written_on[partner.fmu_in_use] += partner.size;
                                    
                                else: # If it is a RECV
                                    #print(str(partner.id) + " out from " + str(partner.fmu_in_use) + "  FUSED")
                                    self.update_fmu_last_cycle(readyMatch.fmu_in_use, partner.endCycle);
                                    self.fmu_circularBuffer.consume_entry(partner.fmu_in_use,
                                                  partner.id);
                                    
                                    partner.READY = True;

                # Delay
                if (
                    rank_in_usage == partner_rank
                ):
                    minToStart = 0;
                    if partner.initialized:
                        minToStart = readyMatch.sep_getEndCycle() + partner.latency;
                    else:
                        minToStart = readyMatch.sep_getEndCycle();

                    inc = minToStart - partner.sep_getBaseCycle();

                    if inc > 0:
                        partner.sep_incrementCycle(inc)
                        self.channel_congestion_time[partner_rank] += inc;

                
            if readyMatch.still_solving_send:
                #print("send to recv -- " + str(readyMatch.id))
                readyMatch.sep_move_RECV_after_SEND();
                self.update_fmu_last_cycle(readyMatch.fmu_in_use, readyMatch.send_endCycle);
                #print(str(readyMatch.id) + " -> " + str(readyMatch.fmu_in_use))
                self.fmu_circularBuffer.insert_entry(readyMatch.fmu_in_use,
                                                 readyMatch.id,
                                                 readyMatch.size);
                self.fmu_data_written_on[readyMatch.fmu_in_use] += readyMatch.size;
                readyMatch = None;


        #print(readyMatch)
        assert readyMatch.still_solving_send == False; # Cant be still solving send

        if readyMatch.READY == False: # It was not solved with another operation
            #print(str(readyMatch.id) + " out from " + str(readyMatch.fmu_in_use))
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








# ***************************************************************
#  ____  _____ _____ _    ____ _____ ___  ____  ___ _   _  ____ 
# |  _ \| ____|  ___/ \  / ___|_   _/ _ \|  _ \|_ _| \ | |/ ___|
# | |_) |  _| | |_ / _ \| |     | || | | | |_) || ||  \| | |  _ 
# |  _ <| |___|  _/ ___ \ |___  | || |_| |  _ < | || |\  | |_| |
# |_| \_\_____|_|/_/   \_\____| |_| \___/|_| \_\___|_| \_|\____|
#                                                               
# ***************************************************************


class Contention_FlexibleMemoryUnit_General(Contention_FlexibleMemoryUnit):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(Contention_FlexibleMemoryUnit_General, self).__init__(nRanks, configfile);
        self.fmu_data_written_on = [0] * self.nFMUs;
        self.fmu_FRR = [[0 for y in range(self.nFMUs)] for x in range(self.nRanks)]


    # FMU Request Queue (FRQ) Timing
    def get_fmu_last_cycle(self, chosenFMU: int)->float:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        return self.fmu_last_cycle_vector[chosenFMU];

    def update_fmu_last_cycle(self, chosenFMU: int, endTime: float)-> None:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        assert self.fmu_last_cycle_vector[chosenFMU] < endTime or math.isclose(self.fmu_last_cycle_vector[chosenFMU], endTime), "what? " + str(endTime) + " < " + str(self.fmu_last_cycle_vector[chosenFMU])
        self.fmu_last_cycle_vector[chosenFMU] = endTime;





    def findReadyMatch(self, matchQ)-> MQ_Match:
        for i in range(0, len(matchQ)):
            if matchQ[i].READY:
                return matchQ[i];
        return None;

    def findEarliestMatch(self, matchQ)-> MQ_Match:
        assert len(matchQ) > 0;
        lowest_cycle = matchQ[0].sep_getBaseCycle();
        li = 0;
        for i in range(0, len(matchQ)):
            if matchQ[i].sep_getBaseCycle() < lowest_cycle:
                lowest_cycle = matchQ[i].sep_getBaseCycle();
                li = i;
        return matchQ[li];

    def findWindow(self, matchQ):
        assert len(matchQ) > 0;
        lowest_cycle = matchQ[0].sep_getBaseCycle();
        li = 0;
        for i in range(0, len(matchQ)):
            if matchQ[i].sep_getBaseCycle() < lowest_cycle:
                lowest_cycle = matchQ[i].sep_getBaseCycle();
                li = i;
        second_lowest_cycle = matchQ[li].sep_getEndCycle();

        assert lowest_cycle < second_lowest_cycle, "second lowest not bigger than lowest?"

        for i in range(0, len(matchQ)):
        
            if matchQ[i].sep_getBaseCycle() < second_lowest_cycle and not math.isclose(matchQ[i].sep_getBaseCycle(), lowest_cycle):
                second_lowest_cycle = matchQ[i].sep_getBaseCycle();
            if matchQ[i].sep_getEndCycle() < second_lowest_cycle:
                second_lowest_cycle = matchQ[i].sep_getEndCycle();

        return lowest_cycle, second_lowest_cycle;

    # Update FMU Request Tracker (FRT)
    #def updateFRT(self, fmu_index, cycle):
    #   if self.fmu_request_tracker[fmu_index] < cycle:
    #       self.fmu_request_tracker[fmu_index] = cycle;
    #   if self.fmu_request_tracker[fmu_index] < cycle:
        

    def incrementFRT(self, fmu_index):
        self.fmu_request_tracker[fmu_index] += 1;
    
    def decrementFRT(self, fmu_index):
        self.fmu_request_tracker[fmu_index] -= 1;
        assert self.fmu_request_tracker[fmu_index] >= 0;


    # - baseCycle to seek FMU
    # - endCycle to mark it as used until endCycle
    def seekIdleFMU(self, baseCycle, endCycle)->int:
        assert endCycle >= baseCycle;
        for i in range(len(self.fmu_request_tracker)):
            #if self.fmu_request_tracker[i] <= baseCycle:
            if self.fmu_request_tracker[i] == 0:
                self.incrementFRT(i);
                return i
                #self.updateFRT(i, endCycle);
                #return i;
        return None;

    # Find lowest used FMU by amount of data
    def chooseFMU_LeastUsedFMU(self)->int:
        fmu_in_use = 0;
        for i in range(0,len(self.fmu_data_written_on)):
            if self.fmu_data_written_on[i] < self.fmu_data_written_on[fmu_in_use]:
                fmu_in_use = i;
        
        self.incrementFRT(fmu_in_use);
        return fmu_in_use



    def chooseFMU(self, readyMatch: MQ_Match, matchQ)-> None:
        assert readyMatch.fmu_in_use == None;
        assert readyMatch.still_solving_send;

        # Window       
        baseCycle = readyMatch.sep_getBaseCycle();
        endCycle = readyMatch.sep_getEndCycle();

        baseCycle, endCycle = self.findWindow(matchQ);

        for i in range(self.nFMUs):
            self.fmu_request_tracker[i] = 0;
            if self.fmu_last_cycle_vector[i] > baseCycle:
                self.fmu_request_tracker[i] += 1;
        
        for i in range(len(matchQ)):
            current_match = matchQ[i];
            if current_match.fmu_in_use is None: # It still needs to be assigned to a FMU
                continue;
            if current_match.sep_getBaseCycle() >= endCycle: # It is out of the window
                continue;
            self.fmu_request_tracker[current_match.fmu_in_use] += 1;

        for i in range(len(matchQ)):
            current_match = matchQ[i];
            if current_match.fmu_in_use is not None: # It has already chosen FMU
                continue;
            if current_match.sep_getBaseCycle() >= endCycle: # It is out of the window
                continue;

            # Try to get an Idle FMU
            current_match.fmu_in_use = self.seekIdleFMU(baseCycle, endCycle);
            if current_match.fmu_in_use != None:
                self.fmu_idle_mapping += 1;
                self.fmu_request_tracker[current_match.fmu_in_use] += 1;
                self.fmu_data_written_on[current_match.fmu_in_use] += current_match.size;
                continue;
            
            # Get FMU using LeastUsedFMU
            current_match.fmu_in_use = self.chooseFMU_LeastUsedFMU();
            self.fmu_request_tracker[current_match.fmu_in_use] += 1;
            #self.updateFRT(current_match.fmu_in_use, endCycle);
            self.fmu_heuristic_mapping += 1;
            self.fmu_data_written_on[current_match.fmu_in_use] += current_match.size;

        return;

        # Try to get an Idle FMU
        readyMatch.fmu_in_use = self.seekIdleFMU(baseCycle, endCycle);
        if readyMatch.fmu_in_use != None:
            self.fmu_idle_mapping += 1;
            self.fmu_data_written_on[readyMatch.fmu_in_use] += readyMatch.size;
            return;

        # Get FMU using LeastUsedFMU
        readyMatch.fmu_in_use = self.chooseFMU_LeastUsedFMU();
        self.updateFRT(readyMatch.fmu_in_use, endCycle);
        self.fmu_idle_mapping += 1;
        self.fmu_data_written_on[readyMatch.fmu_in_use] += readyMatch.size;



    def processContention(self, matchQ) -> MQ_Match:

        #valid_matchesQ = matchQ;

        #print("Valid: " + str(len(valid_matchesQ)) )
        # We might be on a deadlock if there is no valid match on this point
        assert len(matchQ) > 0, "No valid Match was found"

        # Initialize everyone
        for i in range(len(matchQ)):
            currentMatch = matchQ[i];
            if not currentMatch.initialized:
                currentMatch.sep_initializeMatch(self.CommunicationCalculus_Bandwidth(currentMatch.rankS, currentMatch.rankR, currentMatch.size)[0]);

        # find lowest cycle
        readyMatch : MQ_Match
        readyMatch = None;

        readyMatch = self.findReadyMatch(matchQ);

        # If none is ready, lets do this
        while readyMatch == None:

            # Find earliest
            readyMatch = self.findEarliestMatch(matchQ);

            # Initialize if not yet
            if not readyMatch.initialized:
                assert False;
                readyMatch.sep_initializeMatch(self.CommunicationCalculus_Bandwidth(readyMatch.rankS, readyMatch.rankR, readyMatch.size)[0]);

            # Choose FMU is needed
            if readyMatch.fmu_in_use == None:
                self.chooseFMU(readyMatch, matchQ);

            fmu_in_use = readyMatch.fmu_in_use;
            assert fmu_in_use is not None;

            # Adjust time based on the chosen FMU
            minToStart = self.get_fmu_last_cycle(fmu_in_use) + readyMatch.latency;
            inc = minToStart - readyMatch.sep_getBaseCycle();
            if inc > 0:
                readyMatch.sep_incrementCycle(inc);
                self.fmu_congestion_time[fmu_in_use] += inc;


            # ACTIONS
            # - Check if we can move something else to this FMU
            # - Delay other communications as needed
            for j in range(0, len(matchQ)):
                #if j == li:
                #    continue;
                partner: MQ_Match;
                partner = matchQ[j];
                if readyMatch.id == partner.id: # Hey, thats me!
                    continue;

                rank_in_usage = None;
                if readyMatch.still_solving_send:
                    rank_in_usage = readyMatch.rankS;
                else:
                    rank_in_usage = readyMatch.rankR;

                partner_rank = None;
                if partner.still_solving_send:
                    partner_rank = partner.rankS;
                else:
                    partner_rank = partner.rankR;

                # Check
                if partner.fmu_in_use is not None: 
                    if rank_in_usage == partner_rank: # If same Rank
                        if readyMatch.fmu_in_use == partner.fmu_in_use: # If same FMU
                            if (partner.sep_getBaseCycle() - partner.latency) <= readyMatch.sep_getEndCycle(): # If issued before this access endCycle
                                partner.sep_decrementCycle(partner.latency);
                                minToStart = readyMatch.sep_getEndCycle();
                                inc = minToStart - partner.sep_getBaseCycle();
                                if inc > 0:
                                    partner.sep_incrementCycle(inc);

                                if partner.still_solving_send: # IF it is a SEND
                                    partner.sep_move_RECV_after_SEND();
                                    #print(str(partner.id) + " -> " + str(partner.fmu_in_use)  + "  FUSED")
                                    self.update_fmu_last_cycle(readyMatch.fmu_in_use, partner.send_endCycle);
                                    self.fmu_circularBuffer.insert_entry(partner.fmu_in_use,
                                                         partner.id,
                                                         partner.size);
                                    self.fmu_data_written_on[partner.fmu_in_use] += partner.size;
                                    
                                else: # If it is a RECV
                                    #print(str(partner.id) + " out from " + str(partner.fmu_in_use) + "  FUSED")
                                    self.update_fmu_last_cycle(readyMatch.fmu_in_use, partner.endCycle);
                                    self.fmu_circularBuffer.consume_entry(partner.fmu_in_use,
                                                  partner.id);
                                    
                                    partner.READY = True;

                # Delay
                if (
                    rank_in_usage == partner_rank
                ):
                    minToStart = 0;
                    if partner.initialized:
                        minToStart = readyMatch.sep_getEndCycle() + partner.latency;
                    else:
                        minToStart = readyMatch.sep_getEndCycle();

                    inc = minToStart - partner.sep_getBaseCycle();

                    if inc > 0:
                        partner.sep_incrementCycle(inc)
                        self.channel_congestion_time[partner_rank] += inc;

                
            if readyMatch.still_solving_send:
                #print("send to recv -- " + str(readyMatch.id))
                readyMatch.sep_move_RECV_after_SEND();
                self.update_fmu_last_cycle(readyMatch.fmu_in_use, readyMatch.send_endCycle);
                #print(str(readyMatch.id) + " -> " + str(readyMatch.fmu_in_use))
                self.fmu_circularBuffer.insert_entry(readyMatch.fmu_in_use,
                                                 readyMatch.id,
                                                 readyMatch.size);
                self.fmu_data_written_on[readyMatch.fmu_in_use] += readyMatch.size;
                readyMatch = None;


        #print(readyMatch)
        #print(readyMatch)
        assert readyMatch.still_solving_send == False; # Cant be still solving send

        if readyMatch.READY == False: # It was not solved with another operation
            #print(str(readyMatch.id) + " out from " + str(readyMatch.fmu_in_use))
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