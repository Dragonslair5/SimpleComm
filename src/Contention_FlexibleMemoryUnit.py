from MPI_Constants import *
from FMU_CircularBuffer import *
import math
import random





class Contention_FlexibleMemoryUnit:


    @staticmethod
    def getMeTheContentionMethod(nRanks: int, configfile: SimpleCommConfiguration):
        return Contention_FlexibleMemoryUnit_General(nRanks, configfile); 


    # Incoming Recv, to decrement amount of size on an upcoming recv;
    class IncomingRecv:
        def __init__(self, cycle, size, fmu):
            self.cycle = cycle;
            self.size = size;
            self.fmu = fmu;


    def __init__(self, nRanks: int, configfile: SimpleCommConfiguration):


        self.nRanks = nRanks;
        #self.cores_per_node = 1; # TODO: Fix this after you include this option
        self.cores_per_node = configfile.number_of_cores_per_node;
        if self.cores_per_node != 1:
            assert False, "multicore still not available for FMU"
        self.independent_send_recv = True;
        self.eager_protocol_max_size = configfile.eager_protocol_max_size;

        #Override (?)(on topology version of this, it is an override)
        self.network_bandwidth = configfile.internode_bandwidth;
        self.network_latency = configfile.internode_latency;
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
        self.fmu_total_usage_time = [0] * self.nFMUs;
        self.number_of_times_more_than_one_message_was_moved_with_a_single_switch = 0
        self.number_single_switch_sends = 0
        self.number_single_switch_recvs = 0

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = FMU_CircularBuffer(self.nFMUs);

        self.fmu_idle_mapping = 0;
        self.fmu_heuristic_mapping = 0;

        self.list_of_incoming_recvs : typing.List[self.IncomingRecv];
        self.list_of_incoming_recvs = []



    def getMeTotalTimeSpentInPercentage(self):

        total_time = 0;
        for i in range(len(self.fmu_total_usage_time)):
            total_time += self.fmu_total_usage_time[i];
        
        used_percentage = [0] * self.nFMUs;

        for i in range(self.nFMUs):
            used_percentage[i] = (self.fmu_total_usage_time[i]/total_time) * 100
        
        return used_percentage;



# *************************************************************
#  _____ _           _               __  __           _      _ 
# |_   _(_)_ __ ___ (_)_ __   __ _  |  \/  | ___   __| | ___| |
#   | | | | '_ ` _ \| | '_ \ / _` | | |\/| |/ _ \ / _` |/ _ \ |
#   | | | | | | | | | | | | | (_| | | |  | | (_) | (_| |  __/ |
#   |_| |_|_| |_| |_|_|_| |_|\__, | |_|  |_|\___/ \__,_|\___|_|
#                            |___/                             
# *************************************************************

    
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
        #self.fmu_FRR = [[0 for y in range(self.nFMUs)] for x in range(self.nRanks)]



        if configfile.fmu_monitor_incoming_recv:
            self.includeIncomingRecv = self.includeIncomingRecv_Function;
        else:
            self.includeIncomingRecv = self.includeIncomingRecv_Dummy;


        if configfile.fmu_seek_idle_kind == "SIMPLE":
            self.seekIdleFMU = self.seekIdleFMU_Simple;
        elif configfile.fmu_seek_idle_kind == "LEAST_USED_FMU":
            self.seekIdleFMU = self.seekIdleFMU_LeastUsedFMU;
        elif configfile.fmu_seek_idle_kind == "RANDOM":
            self.seekIdleFMU = self.seekIdleFMU_Random;
        elif configfile.fmu_seek_idle_kind == "NONE":
            self.seekIdleFMU = self.seekIdleFMU_None;
        else:
            print( bcolors.FAIL + "ERROR: Unknown fmu seek idle kind:  " + configfile.fmu_seek_idle_kind + bcolors.ENDC);
            sys.exit(1);

        if configfile.fmu_mapping == "STATIC":
            self.chooseFMU_MappingScheme = self.chooseFMU_Static;
        elif configfile.fmu_mapping == "INTERLEAVE":
            self.fmu_interleave = 0;
            self.chooseFMU_MappingScheme = self.chooseFMU_Incremental;
        elif configfile.fmu_mapping == "LEAST_USED_FMU":
            self.chooseFMU_MappingScheme = self.chooseFMU_LeastUsedFMU;
        elif configfile.fmu_mapping == "RANDOM":
            self.chooseFMU_MappingScheme = self.chooseFMU_Random;
        else:
            print( bcolors.FAIL + "ERROR: Unknown fmu mapping scheme:  " + configfile.fmu_mapping + bcolors.ENDC);
            sys.exit(1);


    # FMU Request Queue (FRQ) Timing
    def get_fmu_last_cycle(self, chosenFMU: int)->float:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        return self.fmu_last_cycle_vector[chosenFMU];

    def update_fmu_last_cycle(self, chosenFMU: int, endTime: float)-> None:
        assert chosenFMU < self.nFMUs, "What? FMU " + str(chosenFMU) + " does not exist."
        assert self.fmu_last_cycle_vector[chosenFMU] < endTime or math.isclose(self.fmu_last_cycle_vector[chosenFMU], endTime, rel_tol=0.01, abs_tol=0.001), "what? " + str(endTime) + " < " + str(self.fmu_last_cycle_vector[chosenFMU])
        self.fmu_last_cycle_vector[chosenFMU] = endTime;


    # Find Ready Match
    def findReadyMatch(self, matchQ)-> MQ_Match:
        for i in range(0, len(matchQ)):
            if matchQ[i].READY:
                return matchQ[i];
        return None;


    # Find Earliest match 
    def findEarliestMatch(self, matchQ)-> MQ_Match:
        assert len(matchQ) > 0;
        lowest_cycle = matchQ[0].sep_getBaseCycle();
        li = 0;
        for i in range(0, len(matchQ)):
            if matchQ[i].sep_getBaseCycle() < lowest_cycle:
                lowest_cycle = matchQ[i].sep_getBaseCycle();
                li = i;
        return matchQ[li];

    # Find Window (similar to the one from Kahuna)
    def findWindow(self, matchQ):
        assert len(matchQ) > 0;
        lowest_cycle = matchQ[0].sep_getBaseCycle();
        li = 0;
        for i in range(0, len(matchQ)):
            if matchQ[i].sep_getBaseCycle() < lowest_cycle:
                lowest_cycle = matchQ[i].sep_getBaseCycle();
                li = i;
        second_lowest_cycle = matchQ[li].sep_getEndCycle();

        assert lowest_cycle <= second_lowest_cycle, "second lowest not bigger than lowest? " + str(lowest_cycle) + "   " + str(second_lowest_cycle)

        for i in range(0, len(matchQ)):
        
            if matchQ[i].sep_getBaseCycle() < second_lowest_cycle and not math.isclose(matchQ[i].sep_getBaseCycle(), lowest_cycle):
                second_lowest_cycle = matchQ[i].sep_getBaseCycle();
            if matchQ[i].sep_getEndCycle() < second_lowest_cycle:
                second_lowest_cycle = matchQ[i].sep_getEndCycle();

        return lowest_cycle, second_lowest_cycle;



# ************************************************
#  ____  _____ _____ _  __  ___ ____  _     _____ 
# / ___|| ____| ____| |/ / |_ _|  _ \| |   | ____|
# \___ \|  _| |  _| | ' /   | || | | | |   |  _|  
#  ___) | |___| |___| . \   | || |_| | |___| |___ 
# |____/|_____|_____|_|\_\ |___|____/|_____|_____|
#                                                 
# ************************************************


    def incrementFRT(self, fmu_index):
        self.fmu_request_tracker[fmu_index] += 1;


    def seekIdleFMU_LeastUsedFMU(self, baseCycle, endCycle)->int:
        
        idles = [];
        for i in range(len(self.fmu_request_tracker)):
            if self.fmu_request_tracker[i] == 0:
                idles.append(i);
            
        if len(idles) == 0:
            return None;

        lowest = idles[0];

        for i in range(len(idles)):
            dataOnLowest = self.fmu_data_written_on[lowest]
            dataOnCurrent = self.fmu_data_written_on[idles[i]]
            if dataOnLowest > dataOnCurrent:
                lowest = idles[i];

        return lowest;

    def seekIdleFMU_Simple(self, baseCycle, endCycle)->int:
        for i in range(len(self.fmu_request_tracker)):
            if self.fmu_request_tracker[i] == 0:
                self.incrementFRT(i);
                return i
        return None;


    def seekIdleFMU_Random(self, baseCycle, endCycle)->int:

        idles = [];
        for i in range(len(self.fmu_request_tracker)):
            if self.fmu_request_tracker[i] == 0:
                idles.append(i);
            
        if len(idles) == 0:
            return None;
        
        choose_on_idles = random.randrange(len(idles));

        return idles[choose_on_idles];



    def seekIdleFMU_None(self, baseCycle, endCycle)->int:
        return None;


        


# *******************************************
#  __  __    _    ____  ____ ___ _   _  ____ 
# |  \/  |  / \  |  _ \|  _ \_ _| \ | |/ ___|
# | |\/| | / _ \ | |_) | |_) | ||  \| | |  _ 
# | |  | |/ ___ \|  __/|  __/| || |\  | |_| |
# |_|  |_/_/   \_\_|   |_|  |___|_| \_|\____|
#                                            
# *******************************************


    
    # Find lowest used FMU by amount of data
    def chooseFMU_LeastUsedFMU(self, match: MQ_Match)->int:
        fmu_in_use = 0;
        for i in range(0,len(self.fmu_data_written_on)):
            if self.fmu_data_written_on[i] < self.fmu_data_written_on[fmu_in_use]:
                fmu_in_use = i;
        
        self.incrementFRT(fmu_in_use);
        return fmu_in_use

    # Find FMU by using a global counter (interleave FMUs among requests)
    def chooseFMU_Incremental(self, match: MQ_Match)->int:
        self.fmu_interleave = (self.fmu_interleave + 1) % self.nFMUs;
        self.incrementFRT(self.fmu_interleave)
        return self.fmu_interleave;

    # Find FMU based on the receiver Rank
    def chooseFMU_Static(self, match: MQ_Match)->int:
        self.incrementFRT(match.rankR % self.nFMUs)
        return match.rankR % self.nFMUs;

    def chooseFMU_Random(self, match: MQ_Match)->int:
        chosen_fmu = random.randrange(self.nFMUs)
        self.incrementFRT(chosen_fmu);
        return chosen_fmu;


# ***********************************************************
#   ____ _   _  ___   ___  ____  _____   _____ __  __ _   _ 
#  / ___| | | |/ _ \ / _ \/ ___|| ____| |  ___|  \/  | | | |
# | |   | |_| | | | | | | \___ \|  _|   | |_  | |\/| | | | |
# | |___|  _  | |_| | |_| |___) | |___  |  _| | |  | | |_| |
#  \____|_| |_|\___/ \___/|____/|_____| |_|   |_|  |_|\___/ 
#                                                           
# ***********************************************************


    # Two attempts are made on assigning a FMU to a SEND request
    #   CONFIG: fmu_seek_idle_kind
    #   First attempt:  It tries to assign an IDLE FMU. 
    #                   An IDLE FMU is one that is not switched to any node (access queue is currently empty).
    #                   If no FMU is IDLE, the second attempt is made.
    #                   This first attempt is optional.
    #   
    #   CONFIG: fmu_mapping
    #   Second attempt: It uses some mapping scheme to assign a FMU.
    #                   Note that this same scheme can be used on the first attempt, but considering only IDLE FMUs.
    # 
    # The assigning occurs on SENDs that baseCycle are within a range (referred as window).
    # The process occurs on 6 steps:
    #   1 - Find the window, that is a range starting in a certain *baseCycle* and ending in a certain *endCycle*
    #   2 - Initialize FMU_REQUEST_TRACKER (FRT). This serves to track the size of the access queue
    #       * It initially starts with 0. Each request increments this value.
    #       * This is modified here, checking the leastCycle of each FMU and when seeking IDLE FMUs
    #   3 - Filter matchQ for matches that baseCycle are within the window and put them in the Order Vector
    #   4 - Include incoming RECVs that are within the window and put them in the Order Vector
    #   5 - Sort the Order Vector in temporal order
    #   6 - Decrement data written in FMU if its an incoming RECV
    #       Assign FMUs if its a match, using the following two attepts:
    #       * First, try an IDLE FMU (Optional)
    #       * Second, use the mapping scheme
    #
    def chooseFMU(self, matchQ)-> None:

        current_match: MQ_Match;

        # 1 - Window
        baseCycle, endCycle = self.findWindow(matchQ);
                


        # 2 - Initialize FMU_REQUEST_TRACKER (FRT)
        for i in range(len(self.fmu_request_tracker)):
            self.fmu_request_tracker[i] = 0;            
            if self.get_fmu_last_cycle(i) > baseCycle:
                self.incrementFRT(i);
        
        # 3 - filter matches that are withing the Window
        #     Put them on the Order Vector with 2 arguments
        index_order_vector = []
        for i in range(len(matchQ)):
            current_match = matchQ[i];

            # TODO: Sometimes the window is very short, where baseCycle and endCycle are close enough that math.isclose will make them equal.
            #       We should devise some scheme that takes the size of the window into consideration.
            #       For the moment, we are allowing baseCycle = endCycle due to these too small window sizes. 
            if current_match.sep_getBaseCycle() > endCycle:
                continue;
            #if current_match.sep_getBaseCycle() > endCycle or math.isclose(current_match.sep_getBaseCycle(), endCycle): # It is out of the window
            #    continue;
            index_order_vector.append([i, current_match.sep_getBaseCycle()])

        assert len(index_order_vector) > 0;


        # 4 - Include incoming RECVs that are within the window
        #     Include them in the Order Vector with 3 arguments
        for i in range(len(self.list_of_incoming_recvs)-1, -1, -1):
            incoming_recv: self.IncomingRecv;
            incoming_recv = self.list_of_incoming_recvs[i]
            if incoming_recv.cycle < endCycle: # If the incoming RECV is within the window
                index_order_vector.append([incoming_recv.fmu, incoming_recv.cycle, incoming_recv.size]);
                del self.list_of_incoming_recvs[i];



        # 5 - Sort the Order Vector in temporal order ([1] parameter)
        index_order_vector = sorted(index_order_vector, key = lambda e: e[1])


        # 6 - Choose FMUs to the ones inside the window that still has not been assigned to a FMU
        #     Decrement data written on FMU in case it is an incoming RECV
        for i in range(len(index_order_vector)):
            
            if len(index_order_vector[i]) > 2: #It is an Incoming RECV
                incoming_recv_fmu = index_order_vector[i][0]
                incoming_recv_size = index_order_vector[i][2]
                self.fmu_data_written_on[incoming_recv_fmu] -= incoming_recv_size;
                assert self.fmu_data_written_on[incoming_recv_fmu] >= 0;
                continue;
            
            # It is a match
            current_match = matchQ[index_order_vector[i][0]];
            if current_match.fmu_in_use is not None: # It has already chosen FMU
                self.incrementFRT(current_match.fmu_in_use);
                continue;
            # NOTE: We are allowing baseCycle = endCycle because sometimes the window is too small (few bytes over gigabytes of bandwidth)
            if current_match.sep_getBaseCycle() > endCycle: # It is out of the window
                assert False;
                continue;

            # First Attempt - Try to get an Idle FMU
            current_match.fmu_in_use = self.seekIdleFMU(baseCycle, endCycle);
            if current_match.fmu_in_use != None:
                self.fmu_idle_mapping += 1;
            else:
                # Second Attempt - Get FMU using the fmu mapping scheme
                current_match.fmu_in_use = self.chooseFMU_MappingScheme(current_match);
                self.fmu_heuristic_mapping += 1;
            
            assert current_match.fmu_in_use != None;
            
            
            self.incrementFRT(current_match.fmu_in_use)
            self.fmu_data_written_on[current_match.fmu_in_use] += current_match.size;
            # Include a RECV incoming
            self.includeIncomingRecv(current_match);

        return;



    def includeIncomingRecv_Function(self, current_match):
        incoming_recv_cycle = current_match.send_original_baseCycle + self.network_latency;
        if incoming_recv_cycle < current_match.recv_original_baseCycle:
            incoming_recv_cycle = current_match.recv_original_baseCycle;
        self.list_of_incoming_recvs.append(self.IncomingRecv(cycle=incoming_recv_cycle, size=current_match.size, fmu=current_match.fmu_in_use));

    def includeIncomingRecv_Dummy(self, current_match):
        return;


# *********************************************************
#   ____ ___  _   _ _____ _____ _   _ _____ ___ ___  _   _ 
#  / ___/ _ \| \ | |_   _| ____| \ | |_   _|_ _/ _ \| \ | |
# | |  | | | |  \| | | | |  _| |  \| | | |  | | | | |  \| |
# | |__| |_| | |\  | | | | |___| |\  | | |  | | |_| | |\  |
#  \____\___/|_| \_| |_| |_____|_| \_| |_| |___\___/|_| \_|
#                                                          
# *********************************************************



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


            #assert readyMatch.READY == False
            if readyMatch.READY == True: # If it is READY, it was consumed with another match already.
                break;


            # Choose FMU if needed
            if readyMatch.fmu_in_use == None:
                self.chooseFMU(matchQ);

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
                if partner.READY == True:
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


                #if rank_in_usage == partner_rank:
                #     print(str(rank_in_usage) + " " + readyMatch.send_origin + "   " + str(partner_rank) + " " + partner.send_origin)


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

                                self.number_of_times_more_than_one_message_was_moved_with_a_single_switch += 1;


                                if partner.still_solving_send: # IF it is a SEND
                                    self.number_single_switch_sends += 1;
                                    partner.sep_move_RECV_after_SEND();
                                    self.update_fmu_last_cycle(readyMatch.fmu_in_use, partner.send_endCycle);
                                    self.fmu_circularBuffer.insert_entry(partner.fmu_in_use,
                                                         partner.id,
                                                         partner.size);
                                    
                                else: # If it is a RECV
                                    self.number_single_switch_recvs += 1;
                                    #print(str(partner.id) + " out from " + str(partner.fmu_in_use) + "  FUSED")
                                    self.update_fmu_last_cycle(readyMatch.fmu_in_use, partner.recv_endCycle);
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
                        assert False;
                        minToStart = readyMatch.sep_getEndCycle();

                    inc = minToStart - partner.sep_getBaseCycle();

                    if inc > 0:
                        partner.sep_incrementCycle(inc)
                        self.channel_congestion_time[partner_rank] += inc;

                
            if readyMatch.still_solving_send:
                readyMatch.sep_move_RECV_after_SEND();
                self.update_fmu_last_cycle(readyMatch.fmu_in_use, readyMatch.send_endCycle);
                self.fmu_circularBuffer.insert_entry(readyMatch.fmu_in_use,
                                                 readyMatch.id,
                                                 readyMatch.size);
                readyMatch = None;


        assert readyMatch.still_solving_send == False; # Cant be still solving send

        if readyMatch.READY == False: # It was not solved with another operation
            self.fmu_circularBuffer.consume_entry(readyMatch.fmu_in_use,
                                              readyMatch.id);
            

        # Statistic
        time_spent_on_send = readyMatch.send_endCycle - readyMatch.send_baseCycle;
        time_spent_on_recv = readyMatch.recv_endCycle - readyMatch.recv_baseCycle;
        self.fmu_total_usage_time[readyMatch.fmu_in_use] += time_spent_on_send + time_spent_on_recv;
        # *** 


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