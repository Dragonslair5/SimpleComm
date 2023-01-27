
import sys

from MessageQueue import *
import math

# *********************************************************
#  ____  _                 _       _   _             
# / ___|(_)_ __ ___  _   _| | __ _| |_(_) ___  _ __  
# \___ \| | '_ ` _ \| | | | |/ _` | __| |/ _ \| '_ \ 
#  ___) | | | | | | | |_| | | (_| | |_| | (_) | | | |
# |____/|_|_| |_| |_|\__,_|_|\__,_|\__|_|\___/|_| |_|
#                                                    
#   ___        _               _   
#  / _ \ _   _| |_ _ __  _   _| |_ 
# | | | | | | | __| '_ \| | | | __|
# | |_| | |_| | |_| |_) | |_| | |_ 
#  \___/ \__,_|\__| .__/ \__,_|\__|
#                 |_|              
# *********************************************************

class SimulationOutput:

    def __init__(self):

        # Info that is obtained in the end of the simulation
        self.endTime = 0;
        self.averageMessageSize = 0;
        # **

        # Info that is obtained during the simulation
        #self.minimumMessageSize = 0; # NOTE:Dont know if we ever will need this. Maybe consider second lowest size? Usage of barrier will set the lowest always to 0.
        self.largestMessageSize = 0;
        self.numberOfMessages = 0;
        self.amountOfDataCommunicated = 0;

        self.recv_after_send_has_completed = 0;
        #self.

        self.dict_mpi_called_operations = {key:value for key, value in MPI_Operations.__dict__.items() if key.startswith('MPI_')}
        self.dict_mpi_called_operations = dict.fromkeys(self.dict_mpi_called_operations, 0);

        self.dict_mpi_overhead = {key:value for key, value in MPI_Operations.__dict__.items() if key.startswith('MPI_')}
        self.dict_mpi_overhead = dict.fromkeys(self.dict_mpi_overhead, 0);
        # **

        self.match_list: list[self.SimOutput_SendRecv]
        self.match_list = []
        

    def incrementOperationUsage(self, operation_ID: int):

        operationName=MPI_Operations.getOperationNameByID(operation_ID);
        self.dict_mpi_called_operations[operationName] += 1;


    def inlude_match(self, match: MQ_Match):
        sendrecv = self.SimOutput_SendRecv(match.rankS, match.rankR, match.send_baseCycle, match.send_endCycle,
                                            match.recv_baseCycle, match.recv_endCycle, match.size, match.send_origin);
        self.match_list.append(sendrecv);

    def unload_Matches_on_the_screen(self):
        
        while self.match_list:
            print(self.match_list[0])
            self.match_list.pop(0)

    class SimOutput_SendRecv:

        def __init__(self,
                     rankS: int, 
                     rankR: int, 
                     send_baseCycle: float, 
                     send_endCycle: float, 
                     recv_baseCycle: float, 
                     recv_endCycle: float, 
                     size: int,
                     operation_origin: str
            ):
            

            self.rankS = rankS;
            self.rankR = rankR;
            self.send_baseCycle = send_baseCycle;
            self.send_endCycle = send_endCycle;
            self.recv_baseCycle = recv_baseCycle;
            self.recv_endCycle = recv_endCycle;
            self.size = size;
            self.operation_origin = operation_origin;

        def __str__(self):
            return (str(self.rankS)+","+str(self.rankR)+","+str(self.send_baseCycle)+","+str(self.send_endCycle)+","+str(self.recv_baseCycle)+","+str(self.recv_endCycle)+","+str(self.size)+","+self.operation_origin)



# *********************************************************
#  ____  _                 _       _   _             
# / ___|(_)_ __ ___  _   _| | __ _| |_(_) ___  _ __  
# \___ \| | '_ ` _ \| | | | |/ _` | __| |/ _ \| '_ \ 
#  ___) | | | | | | | |_| | | (_| | |_| | (_) | | | |
# |____/|_|_| |_| |_|\__,_|_|\__,_|\__|_|\___/|_| |_|
#                                                    
#  _____             _            
# | ____|_ __   __ _(_)_ __   ___ 
# |  _| | '_ \ / _` | | '_ \ / _ \
# | |___| | | | (_| | | | | |  __/
# |_____|_| |_|\__, |_|_| |_|\___|
#              |___/              
# *********************************************************


class SimpleCommEngine:

    def __init__(self, nRanks, configfile: str,  verbose = True):
        self.list_ranks : list[Rank]
        self.list_ranks = []
        self.saveState = [0] * nRanks;
        self.nSteps = 0;
        self.verbose = verbose;
        self.ended = False;
        self.config : SimpleCommConfiguration;
        self.config = SimpleCommConfiguration(configfile);
        self.MQ : MessageQueue;
        self.MQ = MessageQueue(nRanks, self.config)
        #self.show_progress = self.config.show_progress;
        self.show_progress_level = self.config.show_progress_level;
        self.print_communication_trace = self.config.print_communication_trace; # Used on print_blank

        self.showResults = None;
        if self.verbose:
            self.showResults = self.print_verbose;
        elif self.show_progress_level == "blank":
            self.showResults = self.print_blank;
            if self.print_communication_trace:
                print("rankS,rankR,SbaseCycle,SendCycle,RbaseCycle,RendCycle,size,opOrigin");
        elif self.show_progress_level == "perrank":
            self.showResults = self.print_progress_per_rank;
        elif self.show_progress_level == "overall":
            self.showResults = self.print_overall;
        else:
            print( bcolors.FAIL + "ERROR: Unknown show results option:  " + self.show_progress_level + bcolors.ENDC);
            sys.exit(1);

        self.simOutput: SimulationOutput;
        self.simOutput = SimulationOutput();


    def read_traces(self, nRanks, traces_path):

        for rank in range(1, nRanks+1, 1):
            trace = [];
            rankFile = open(traces_path+"/"+"rank-"+str(rank)+".txt", "r");
            for line in rankFile:
                stripped_line = line.strip();
                line_list = stripped_line.split();
                trace.append(line_list);
            rankFile.close();

            aux_rank = Rank(nRanks, rank-1, trace, self.config);

            self.list_ranks.append(aux_rank);



    def simulate(self):
        self.nSteps = self.nSteps + 1;
        END = 0 # When END equals number of Ranks, it is the end
        
        self.MQ.op_message = "" # Clearing op_message (used for debugging coolective operations)

        # Step forward
        for ri in range(len(self.list_ranks)):
            
            # Try to progress on simulation (step)
            sr_list = self.list_ranks[ri].step(len(self.list_ranks));

            if sr_list is not None:
                if isinstance(sr_list, SendRecv):
                    self.MQ.includeSendRecv(sr_list);
                else:
                    while(sr_list):
                        self.MQ.includeSendRecv(sr_list.pop(0)); 
            
            
            if self.list_ranks[ri].state == Rank.S_ENDED:
                END = END + 1

        if END == len(self.list_ranks):
            self.ended = True;
            return END;

        # Process MatchQueue
        match: MQ_Match;
        match = self.MQ.processMatchQueue(self.list_ranks);
        #assert match is not None, "No match was found"
        if match is not None:
            #print(match)
            #print(" SR " + str(match.rankS) + " --> " + str(match.rankR))

            # General Statistics
            if self.show_progress_level == "blank" and self.print_communication_trace:
                self.simOutput.inlude_match(match);

            # ********* SEND
            if match.blocking_send:
                assert math.isclose(match.send_original_baseCycle, match.send_endCycle) or match.send_original_baseCycle < match.send_endCycle
                # Eager Protocol - skip this assert if Eager Protocol is used
                if not self.MQ.topology.independent_send_recv and match.size >= self.config.eager_protocol_max_size:
                    # * 1.100 due to floating point imprecision (purely empirical value)
                    #assert self.list_ranks[match.rankS].cycle <= match.send_endCycle * 1.100, "Rank:" + str(match.rankS) + ": cycle " + str(self.list_ranks[match.rankS].cycle) + " cycle " + str(match.send_endCycle);
                    assert math.isclose(self.list_ranks[match.rankS].cycle, match.send_endCycle, abs_tol=0.001) or self.list_ranks[match.rankS].cycle < match.send_endCycle, "Rank:" + str(match.rankS) + ": cycle " + str(self.list_ranks[match.rankS].cycle) + " cycle " + str(match.send_endCycle);

                if match.send_endCycle > self.list_ranks[match.rankS].cycle:
                    self.list_ranks[match.rankS].includeHaltedTime(self.list_ranks[match.rankS].cycle, match.send_endCycle, match.send_operation_ID);
                    self.list_ranks[match.rankS].cycle = match.send_endCycle;
                if self.MQ.blockablePendingMessage[match.rankS] == 0 and self.list_ranks[match.rankS].canGoToNormal() and self.list_ranks[match.rankS].i_am_blocked_by_standard_send_or_recv:
                    self.list_ranks[match.rankS].state = Rank.S_NORMAL;
                    self.list_ranks[match.rankS].i_am_blocked_by_standard_send_or_recv = False;
            else:
                self.list_ranks[match.rankS].include_iSendRecvConclusion(match.tag, match.send_endCycle, match.send_operation_ID);
            
            # Statistics
            self.list_ranks[match.rankS].amountOfCommunications = self.list_ranks[match.rankS].amountOfCommunications + 1;
            self.list_ranks[match.rankS].amountOfDataOnCommunication = self.list_ranks[match.rankS].amountOfDataOnCommunication + match.size;
            if match.size > self.list_ranks[match.rankS].largestDataOnASingleCommunication:
                self.list_ranks[match.rankS].largestDataOnASingleCommunication = match.size;
            
            # Collective
            if MPI_Operations.isCollectiveOperation(match.send_operation_ID):
                self.list_ranks[match.rankS].concludeCollectiveSendRecv();


            # ********* RECV
            if match.blocking_recv:
                #assert match.recv_original_baseCycle <= match.recv_endCycle
                assert math.isclose(match.recv_original_baseCycle, match.recv_endCycle) or match.recv_original_baseCycle < match.recv_endCycle
                if not self.MQ.topology.independent_send_recv:
                    # * 1.100 due to floating point imprecision (purely empirical value)
                    #assert self.list_ranks[match.rankR].cycle <= match.recv_endCycle  * 1.100, "Rank:" + str(match.rankR) + ": cycle " + str(self.list_ranks[match.rankR].cycle) + " cycle " + str(match.recv_endCycle);
                    assert math.isclose(self.list_ranks[match.rankR].cycle, match.recv_endCycle, abs_tol=0.001) or self.list_ranks[match.rankR].cycle < match.recv_endCycle, "Rank:" + str(match.rankR) + ": cycle " + str(self.list_ranks[match.rankR].cycle) + " cycle " + str(match.recv_endCycle);
                if match.recv_endCycle > self.list_ranks[match.rankR].cycle:
                    self.list_ranks[match.rankR].includeHaltedTime(self.list_ranks[match.rankR].cycle, match.recv_endCycle, match.recv_operation_ID);
                    self.list_ranks[match.rankR].cycle = match.recv_endCycle;
                if self.MQ.blockablePendingMessage[match.rankR] == 0 and self.list_ranks[match.rankR].canGoToNormal() and self.list_ranks[match.rankR].i_am_blocked_by_standard_send_or_recv:
                    self.list_ranks[match.rankR].state = Rank.S_NORMAL;
                    self.list_ranks[match.rankR].i_am_blocked_by_standard_send_or_recv = False;
            else:
                self.list_ranks[match.rankR].include_iSendRecvConclusion(match.tag, match.recv_endCycle, match.recv_operation_ID);
            
            # Statistics
            self.list_ranks[match.rankR].amountOfCommunications = self.list_ranks[match.rankR].amountOfCommunications + 1;
            self.list_ranks[match.rankR].amountOfDataOnCommunication = self.list_ranks[match.rankR].amountOfDataOnCommunication + match.size
            if match.size > self.list_ranks[match.rankR].largestDataOnASingleCommunication:
                self.list_ranks[match.rankR].largestDataOnASingleCommunication = match.size;

            self.simOutput.numberOfMessages += 1;
            self.simOutput.amountOfDataCommunicated += match.size;
            if match.size > self.simOutput.largestMessageSize:
                self.simOutput.largestMessageSize = match.size;

            self.simOutput.incrementOperationUsage(match.send_operation_ID);
            self.simOutput.incrementOperationUsage(match.recv_operation_ID);

            if ( 
               (match.recv_original_baseCycle > match.send_conclusionCycle)
               ):
               self.simOutput.recv_after_send_has_completed += 1;
            # **

            # Collective
            if MPI_Operations.isCollectiveOperation(match.recv_operation_ID):
                self.list_ranks[match.rankR].concludeCollectiveSendRecv();

            

            #del match;


        return END;




# *********************************************************
#  ____  ____  ___ _   _ _____ ___ _   _  ____ 
# |  _ \|  _ \|_ _| \ | |_   _|_ _| \ | |/ ___|
# | |_) | |_) || ||  \| | | |  | ||  \| | |  _ 
# |  __/|  _ < | || |\  | | |  | || |\  | |_| |
# |_|   |_| \_\___|_| \_| |_| |___|_| \_|\____|
#                                              
#  _____ _   _ _   _  ____ _____ ___ ___  _   _ ____  
# |  ___| | | | \ | |/ ___|_   _|_ _/ _ \| \ | / ___| 
# | |_  | | | |  \| | |     | |  | | | | |  \| \___ \ 
# |  _| | |_| | |\  | |___  | |  | | |_| | |\  |___) |
# |_|    \___/|_| \_|\____| |_| |___\___/|_| \_|____/ 
#                                                     
# *********************************************************




    def print_verbose(self):
        print(bcolors.OKGREEN + "Result - step " + str(self.nSteps) + bcolors.OKPURPLE + self.MQ.op_message + bcolors.ENDC)
        for ri in range(len(self.list_ranks)):
            rank = self.list_ranks[ri];
            if self.saveState[ri] != rank.cycle:
                print(bcolors.OKCYAN, end='');
            print("{: <15}".format(rank.getCurrentStateName()), end='');
            print(bcolors.ENDC, end='');
        print("");
        for ri in range(len(self.list_ranks)):
            rank = self.list_ranks[ri];
            if self.saveState[ri] != rank.cycle:
                print(bcolors.OKCYAN, end='');
            print("{: <15}".format(str(self.MQ.currentPosition[ri])), end='');
            print(bcolors.ENDC, end='');
        print("");
        for ri in range(len(self.list_ranks)):
            rank = self.list_ranks[ri];
            if self.saveState[ri] != rank.cycle:
                print(bcolors.OKCYAN, end='');
            print("{: <15}".format(rank.current_operation), end='');
            print(bcolors.ENDC, end='');
        print("");
        for ri in range(len(self.list_ranks)):
            rank = self.list_ranks[ri];
            if self.saveState[ri] != rank.cycle:
                self.saveState[ri] = rank.cycle;
                print(bcolors.OKCYAN, end='');
            #print("{: <15e}".format(rank.cycle), end='');
            print("{: <15.6f}".format(rank.cycle), end='');
            print(bcolors.ENDC, end='');
        print("");
        input(""); # Press key to advance


    def print_progress_per_rank(self):
        print("", end= '\r', flush=True);
        #print(str(self.nSteps) + " | ")
        for ri in range(len(self.list_ranks)):
            rank: Rank;
            rank = self.list_ranks[ri];
            if self.saveState[ri] != rank.cycle:
                print(bcolors.OKCYAN, end='');
            if rank.state == Rank.S_WAITING:
                print(bcolors.FAIL, end='');
            print(str(rank.index) + "/" + str(len(rank.trace)) + "--", end='')
            print("{: <4.1f}".format( float(rank.index)/float(len(rank.trace)) * 100 ), end='');
            #print("Sonic", end='')
            print(" {:15s}".format( rank.current_operation.split("-")[0] ), end='');
            #print(rank.current_operation , end='');
            print(bcolors.ENDC, end='');
            self.saveState[ri] = rank.cycle;
        if self.ended:
            print("", end= '\r', flush=True); # Go back to the start of the line
            sys.stdout.write("\x1b[2K") # Erase the line
            biggestCycle = self.list_ranks[0].cycle;
            for ri in range(1, len(self.list_ranks)):
                if self.list_ranks[ri].cycle > biggestCycle:
                    biggestCycle =  self.list_ranks[ri].cycle;
            print(biggestCycle); 



    # TODO: Update this to match the information from print_blank
    def print_overall(self):
        
        print("", end= '\r', flush=True);
        total: int
        total = 0
        partial: int
        partial = 0
        
        for i in range(0, len(self.list_ranks)):
            total = total + self.list_ranks[i].trace_size;
            partial = partial + self.list_ranks[i].index;
        
        progress: float;
        progress = (float(partial) / float(total)) * 100;
        #print(str(progress) + "%", end='')
        print("-- {:.2f}".format(progress) + "% --", end='')
        
        if not self.ended:
            return None;

        print("", end= '\r', flush=True);
        print("topology:"+self.config.topology)
        print("booster_factor:"+str(self.config.booster_factor))

        biggestCycle = self.list_ranks[0].cycle;
        for ri in range(1, len(self.list_ranks)):
            if self.list_ranks[ri].cycle > biggestCycle:
                biggestCycle =  self.list_ranks[ri].cycle;
        #print(biggestCycle);
        print("biggest:"+str(biggestCycle))
        total_time = 0
        halted_time = 0
        for ri in range(0, len(self.list_ranks)):
            total_time = total_time + self.list_ranks[ri].cycle;
            halted_time = halted_time + self.list_ranks[ri].timeHaltedDueCommunication;
        halted_time_percentage = (halted_time / total_time) * 100;
        print("halted_time_percentage:" + "{:.2f}".format(halted_time_percentage) )
        biggest_buffer_size = 0;
        if (isinstance(self.MQ.topology, TopHybrid) or
            isinstance(self.MQ.topology, TopFreeMemoryUnit)
        ):
            biggest_buffer_size = self.MQ.topology.fmu_circularBuffer.biggest_buffer_size;
            print("fmu_contention_time:", end='')
            print(str(self.MQ.topology.fmu_congestion_time[0]), end='');
            for i in range(1, self.MQ.topology.nFMUs):
                print(","+str(self.MQ.topology.fmu_congestion_time[i]), end='')
            print("");
        print("biggest_buffer_size:"+str(biggest_buffer_size));
        hybrid_pivot_value = -1;
        proportion_fmu_usage = 0.0;
        if isinstance(self.MQ.topology, TopHybrid):
            hybrid_pivot_value = self.MQ.topology.pivotValue;
            proportion_fmu_usage = (self.MQ.topology.total_messages_fmu / self.MQ.topology.total_messages) * 100;
        if isinstance(self.MQ.topology, TopFreeMemoryUnit):
            proportion_fmu_usage = 100.0
        print("hybrid_pivot_value:"+str(int(hybrid_pivot_value)));
        print("proportion_fmu_usage:"+str(proportion_fmu_usage))
        print("rank,endTime,haltedTime,percentHaltedTime,numCommunications,averageMessageSize,largestData")
        for ri in range(0, len(self.list_ranks)):

            endTime = self.list_ranks[ri].cycle;
            haltedTime = self.list_ranks[ri].timeHaltedDueCommunication;
            numCommunications = self.list_ranks[ri].amountOfCommunications;
            averageCommunicationSize = self.list_ranks[ri].amountOfDataOnCommunication / numCommunications;
            largestDataOnSingleCommunication = self.list_ranks[ri].largestDataOnASingleCommunication;

            print("rank"+str(ri), end=',')
            print(str(endTime), end=',')
            print(str(haltedTime), end=',')
            print("{:.2f}".format( ((haltedTime/endTime)*100) ), end=',')
            print(str(numCommunications), end=',')
            print(str(averageCommunicationSize), end=',')
            print(str(largestDataOnSingleCommunication))

            print("H_"+"rank"+str(ri), end='')
            for key, value in self.list_ranks[ri].dict_mpi_overhead.items():
                halted_dictionary = self.list_ranks[ri].dict_mpi_overhead;
                haltedTime = halted_dictionary[key];
                if haltedTime == 0:
                    continue;
                haltedTime_percentage = (haltedTime / endTime)*100;
                print("," + key + ":" + "{:.2f}".format(haltedTime_percentage), end='')
            print("")




# ********************************
#  ____  _        _    _   _ _  __
# | __ )| |      / \  | \ | | |/ /
# |  _ \| |     / _ \ |  \| | ' / 
# | |_) | |___ / ___ \| |\  | . \ 
# |____/|_____/_/   \_\_| \_|_|\_\
#                                 
# ********************************
# This is the main way/most updated printing function
# It has three parts
#   1 - (if print_communication_trace == True ) print each match on the screen
#       This might produce a huge trace, so becareful
#   2 - If simulation has not ended, return and print nothing
#   3 - Simulation has ended, so print the results
    def print_blank(self):

        # Part 1
        # **** Communication Trace
        # Activated with --> print_communication_trace = False
        # NOTE: This might be huge! Becareful using it.         
        self.simOutput.unload_Matches_on_the_screen();
        # ***

        # Part 2
        # While simulation is running, return
        if not self.ended:
            return None;

        # Part 3
        # Simulation has ended, print the result

        if self.print_communication_trace:
            print("#");
        print("[GENERAL]")
        
        # Number of Ranks
        print("number_of_ranks:"+str(self.MQ.topology.nRanks));
        # TOPOLOGY
        print("topology:"+self.config.topology)
        # BOOSTER FACTOR
        print("booster_factor:"+str(self.config.booster_factor))

        # END TIME (biggest ending cycle)
        biggestCycle = self.list_ranks[0].cycle;
        for ri in range(1, len(self.list_ranks)):
            if self.list_ranks[ri].cycle > biggestCycle:
                biggestCycle =  self.list_ranks[ri].cycle;
        print("biggest:"+str(biggestCycle))
  
  
        # HALTED TIME (Idleness)
        total_time = 0
        halted_time = 0
        for ri in range(0, len(self.list_ranks)):
            total_time = total_time + self.list_ranks[ri].cycle;
            halted_time = halted_time + self.list_ranks[ri].timeHaltedDueCommunication;
        halted_time_percentage = (halted_time / total_time) * 100;
        print("halted_time_percentage:" + "{:.2f}".format(halted_time_percentage) )

        # Number of Messages
        number_of_messages=self.simOutput.numberOfMessages;
        print("number_of_messages:"+str(number_of_messages));
        # Average Message Size
        average_message_size=self.simOutput.amountOfDataCommunicated/number_of_messages;
        print("average_message_size:"+ str(average_message_size));

        # Largest Message Size
        largest_message_size=self.simOutput.largestMessageSize;
        print("largest_message_size:"+str(largest_message_size));


        # Percentage of RECVs initiated after SEND conclusion
        # TODO: This might be wrong
        percentage_of_recvs_after_send_conclusion = (self.simOutput.recv_after_send_has_completed / number_of_messages) * 100;
        print("recv_after_send_conclusion:" + str(percentage_of_recvs_after_send_conclusion));
        
        # Most called MPI operation
        dic_mpi_operations = self.simOutput.dict_mpi_called_operations;
        print("most_called_mpi_operation:"+max(dic_mpi_operations, key=dic_mpi_operations.get));

        # MPI operation that most caused idleness
        dic_mpi_haltness = self.simOutput.dict_mpi_overhead;
        for ri in range(0, len(self.list_ranks)):
            rank_dic = self.list_ranks[ri].dict_mpi_overhead
            dic_mpi_haltness = {k: dic_mpi_haltness.get(k, 0) + rank_dic.get(k, 0) for k in set(dic_mpi_haltness) | set(rank_dic)}        
        print("most_idleness_source_mpi_operation:", max(dic_mpi_haltness, key=dic_mpi_haltness.get))



        # FMU Specific 
        # ************
        print("[FMU]")
        print("number_of_fmus:"+str(self.config.number_of_FMUs))
        print("fmu_mapping:"+self.config.fmu_mapping)
        print("fmu_seek_idle_kind:"+str(self.config.fmu_seek_idle_kind))
        biggest_buffer_size = 0;
        if (isinstance(self.MQ.topology, TopHybrid) or
            isinstance(self.MQ.topology, TopFreeMemoryUnit)
        ):
            biggest_buffer_size = self.MQ.topology.fmu_circularBuffer.biggest_buffer_size;
        # Biggest Buffer Size (Qdata size on FMU)
        print("biggest_buffer_size:"+str(biggest_buffer_size));
        
        # Pivot Value
        # % of FMU usage
        hybrid_pivot_value = -1;
        proportion_fmu_usage = 0.0;
        if isinstance(self.MQ.topology, TopHybrid):
            hybrid_pivot_value = self.MQ.topology.pivotValue;
            proportion_fmu_usage = (self.MQ.topology.total_messages_fmu / self.MQ.topology.total_messages) * 100;
        if isinstance(self.MQ.topology, TopFreeMemoryUnit):
            proportion_fmu_usage = 100.0
        # Hybrid pivot Value (HYBRID)
        print("hybrid_pivot_value:"+str(int(hybrid_pivot_value)));
        # Proportion of FMU usage (HYBRID)
        print("proportion_fmu_usage:"+str(proportion_fmu_usage))

        if isinstance(self.MQ.topology, TopHybrid) or isinstance(self.MQ.topology, TopFreeMemoryUnit):
            print("fmu_data_written:"+str(self.MQ.topology.top_fmu.fmu_data_written_on))
            print("fmu_congestion_time:"+str(self.MQ.topology.top_fmu.fmu_congestion_time))
            print("fmu_channel_congestion_time:"+str(self.MQ.topology.top_fmu.channel_congestion_time))
            print("fmu_idle_mapping:"+str(self.MQ.topology.top_fmu.fmu_idle_mapping))
            print("fmu_heuristic_mapping:"+str(self.MQ.topology.top_fmu.fmu_heuristic_mapping))
            percentual_idle_served = (self.MQ.topology.top_fmu.fmu_idle_mapping / self.simOutput.numberOfMessages) * 100
            print("fmu_idle_served_percentage:"+str(percentual_idle_served))
            assert (self.MQ.topology.top_fmu.fmu_idle_mapping + self.MQ.topology.top_fmu.fmu_heuristic_mapping) == self.simOutput.numberOfMessages



        print("[PER RANK]")
        # Per Rank Results
        # **************************************************************************************************
        # Header for individual Rank stats
        print("rank,endTime,haltedTime,percentHaltedTime,numCommunications,averageMessageSize,largestData")
        for ri in range(0, len(self.list_ranks)):

            endTime = self.list_ranks[ri].cycle;
            haltedTime = self.list_ranks[ri].timeHaltedDueCommunication;
            numCommunications = self.list_ranks[ri].amountOfCommunications;
            averageCommunicationSize = self.list_ranks[ri].amountOfDataOnCommunication / numCommunications;
            largestDataOnSingleCommunication = self.list_ranks[ri].largestDataOnASingleCommunication;

            # Rank ID
            print("rank"+str(ri), end=',')
            # END TIME
            print(str(endTime), end=',')
            # Halted Time (Idleness)
            print(str(haltedTime), end=',')
            # Halted Time Percentage
            print("{:.2f}".format( ((haltedTime/endTime)*100) ), end=',')
            # Number of communications
            print(str(numCommunications), end=',')
            # Average Communication Size
            print(str(averageCommunicationSize), end=',')
            # Largest Message Size
            print(str(largestDataOnSingleCommunication))

            # Rank ID (For individual haltness [idleness] of individual MPI operation)
            print("H_"+"rank"+str(ri), end='')
            for key, value in self.list_ranks[ri].dict_mpi_overhead.items():
                halted_dictionary = self.list_ranks[ri].dict_mpi_overhead;
                haltedTime = halted_dictionary[key];
                if haltedTime == 0:
                    continue;
                haltedTime_percentage = (haltedTime / endTime)*100;
                print("," + key + ":" + "{:.2f}".format(haltedTime_percentage), end='')
            print("")
        
