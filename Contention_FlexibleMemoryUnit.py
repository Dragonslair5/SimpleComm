from MPI_Constants import *
from FMU_CircularBuffer import *




class Contention_FlexibleMemoryUnit:


    def __init__(self, nRanks, configfile: SimpleCommConfiguration):


        self.nRanks = nRanks;
        self.cores_per_node = 1; # TODO: Fix this after you include this option
        self.independent_send_recv = True;
        self.eager_protocol_max_size = configfile.eager_protocol_max_size;


        self.nFMUs = configfile.number_of_FMUs;
        assert self.nFMUs > 0, "Number of Free Memory Units needs to be at least 1 when using FMUs topology"
        self.fmu_interleave = 0;
        self.fmu_last_cycle_vector = [0] * self.nFMUs;
        self.fmu_congestion_time = [0] * self.nFMUs;

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = FMU_CircularBuffer(self.nFMUs);

        #Override (?)(on topology version of this, it is an override)
        self.interLatency = configfile.fmu_latency;
        self.interBandwidth = configfile.fmu_bandwidth;

        self.isThereConflict = None;
        self.chooseFMU = None;
        self.update_fmu_last_cycle = None;
        self.get_fmu_last_cycle = None;
        if configfile.fmu_contention_model == "STATIC":
            self.isThereConflict = self.isThereConflict_Static;
            self.chooseFMU = self.chooseFMU_static;
            # ON / ON
            self.update_fmu_last_cycle = self.update_fmu_last_cycle_TURNED_ON;
            self.get_fmu_last_cycle = self.get_fmu_last_cycle_TUNED_ON;
        elif configfile.fmu_contention_model == "NO_CONFLICT":
            self.isThereConflict = self.isThereConflict_NoConflict;
            self.chooseFMU = self.chooseFMU_oracle;
            # OFF / OFF
            self.update_fmu_last_cycle = self.update_fmu_last_cycle_TURNED_OFF;
            self.get_fmu_last_cycle = self.get_fmu_last_cycle_TUNED_OFF;
        elif configfile.fmu_contention_model == "INTERLEAVE":
            self.isThereConflict = self.isThereConflict_Interleave;
            self.chooseFMU = self.chooseFMU_interleave;
            # ON / ON
            self.update_fmu_last_cycle = self.update_fmu_last_cycle_TURNED_ON;
            self.get_fmu_last_cycle = self.get_fmu_last_cycle_TUNED_ON;
        else:
            print( bcolors.FAIL + "ERROR: Unknown fmu contention model:  " + configfile.fmu_contention_model + bcolors.ENDC);
            sys.exit(1);




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


# **********************************************************************
#   ____            _             _   _               _   _ _   _ _     
#  / ___|___  _ __ | |_ ___ _ __ | |_(_) ___  _ __   | | | | |_(_) |___ 
# | |   / _ \| '_ \| __/ _ \ '_ \| __| |/ _ \| '_ \  | | | | __| | / __|
# | |__| (_) | | | | ||  __/ | | | |_| | (_) | | | | | |_| | |_| | \__ \
#  \____\___/|_| |_|\__\___|_| |_|\__|_|\___/|_| |_|  \___/ \__|_|_|___/
#                                                                       
# **********************************************************************


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
                valid_matchesQ[i].sep_initializeMatch(self.CommunicationCalculus_Bandwidth(valid_matchesQ[i].rankS, valid_matchesQ[i].rankR, valid_matchesQ[i].size)[0]);
                chosenFMU = self.chooseFMU(valid_matchesQ[i].rankR);
                valid_matchesQ[i].fmu_in_use = chosenFMU;
                if chosenFMU < self.nFMUs:
                    minToStart = self.get_fmu_last_cycle(chosenFMU) + valid_matchesQ[i].latency;
                    inc = minToStart - valid_matchesQ[i].sep_getBaseCycle();
                    if inc > 0:
                        valid_matchesQ[i].sep_incrementCycle(inc);
                        self.fmu_congestion_time[chosenFMU] = self.fmu_congestion_time[chosenFMU] + inc;



# **************************************************
#  ____                              
# |  _ \ _ __ ___   ___ ___  ___ ___ 
# | |_) | '__/ _ \ / __/ _ \/ __/ __|
# |  __/| | | (_) | (_|  __/\__ \__ \
# |_|   |_|  \___/ \___\___||___/___/
#                                    
#   ____            _             _   _             
#  / ___|___  _ __ | |_ ___ _ __ | |_(_) ___  _ __  
# | |   / _ \| '_ \| __/ _ \ '_ \| __| |/ _ \| '_ \ 
# | |__| (_) | | | | ||  __/ | | | |_| | (_) | | | |
#  \____\___/|_| |_|\__\___|_| |_|\__|_|\___/|_| |_|
#                                                   
# **************************************************


    def processContention(self, matchQ) -> MQ_Match:

        valid_matchesQ = matchQ;

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

        # If readyMatch is None, it does not exist... what happened?        
        assert readyMatch is not None, "ready match is not presented on matches queues"


        return readyMatch;

