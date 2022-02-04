
import sys

from MessageQueue import *




class SimulationOutput:

    def __init__(self):
        self.endTime = 0;
        self.averageMessageSize = 0;
        self.minimumMessageSize = 0;
        self.largestMessageSize = 0;
        self.numberOfMessages = 0;



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

        self.showResults = None;
        if self.verbose:
            self.showResults = self.print_verbose;
        elif self.show_progress_level == "blank":
            self.showResults = self.print_blank;
        elif self.show_progress_level == "perrank":
            self.showResults = self.print_progress_per_rank;
        elif self.show_progress_level == "overall":
            self.showResults = self.print_overall;
        else:
            print( bcolors.FAIL + "ERROR: Unknown show results option:  " + self.show_progress_level + bcolors.ENDC);
            sys.exit(1);

    #def configure(self, configfile: str):
    #    self.config = SimpleCommConfiguration(configfile);
    #    #print("hehe")



    def read_traces(self, nRanks, traces_path):

        for rank in range(1, nRanks+1, 1):
            trace = [];
            rankFile = open(traces_path+"/"+"rank-"+str(rank)+".txt", "r");
            for line in rankFile:
                stripped_line = line.strip();
                line_list = stripped_line.split();
                trace.append(line_list);
            rankFile.close();

            #if self.verbose:
            #    print( bcolors.OKBLUE + "Rank-" + str(rank) + bcolors.ENDC);
            #    print(trace);

            aux_rank = Rank(rank-1, trace, self.config);

            self.list_ranks.append(aux_rank);



    def simulate(self):
        self.nSteps = self.nSteps + 1;
        END = 0 # When END equals number of Ranks, it is the end
        
        self.MQ.op_message = "" # Clearing op_message (used for debugging coolective operations)

        # Step forward
        for ri in range(len(self.list_ranks)):
            # Try to progress on simulation (step)
            operation = self.list_ranks[ri].step(len(self.list_ranks));
            if operation is not None:
                if isinstance(operation, SendRecv):
                    self.MQ.includeSendRecv(operation);
                elif isinstance(operation, MQ_Bcast_entry):
                    self.MQ.include_Bcast(operation, len(self.list_ranks));
                elif isinstance(operation, MQ_Barrier_entry):
                    self.MQ.include_Barrier(operation, len(self.list_ranks));
                elif isinstance(operation, MQ_Reduce_entry):
                    self.MQ.include_Reduce(operation, len(self.list_ranks));
                elif isinstance(operation, MQ_Allreduce_entry):
                    self.MQ.include_Allreduce(operation, len(self.list_ranks));
                elif isinstance(operation, MQ_Alltoall_entry):
                    self.MQ.include_Alltoall(operation, len(self.list_ranks));
                elif isinstance(operation, MQ_Alltoallv_entry):
                    self.MQ.include_Alltoallv(operation, len(self.list_ranks));
            if self.list_ranks[ri].state == Rank.S_ENDED:
                END = END + 1

        if END == len(self.list_ranks):
            self.ended = True;
            return END;

        # Process Collective Operations
        self.MQ.processCollectiveOperations(self.config);

        # Process MatchQueue
        match: MQ_Match;
        match = self.MQ.processMatchQueue(self.list_ranks);
        #assert match is not None, "No match was found"
        if match is not None:
            #print(" SR " + str(match.rankS) + " --> " + str(match.rankR))
            # ********* SEND
            if match.blocking_send:
                assert self.list_ranks[match.rankS].cycle <= match.endCycle, str(match.rankS) + " - cycle " + str(self.list_ranks[match.rankS].cycle) + " cycle " + str(match.endCycle);
                self.list_ranks[match.rankS].includeHaltedTime(self.list_ranks[match.rankS].cycle, match.endCycle);
                self.list_ranks[match.rankS].cycle = match.endCycle;
                if self.MQ.blockablePendingMessage[match.rankS] == 0:
                    self.list_ranks[match.rankS].state = Rank.S_NORMAL;
            else:
                self.list_ranks[match.rankS].include_iSendRecvConclusion(match.tag, match.endCycle);
            
            # Statistics
            self.list_ranks[match.rankS].amountOfCommunications = self.list_ranks[match.rankS].amountOfCommunications + 1;
            self.list_ranks[match.rankS].amountOfDataOnCommunication = self.list_ranks[match.rankS].amountOfDataOnCommunication + match.size;
            if match.size > self.list_ranks[match.rankS].largestDataOnASingleCommunication:
                self.list_ranks[match.rankS].largestDataOnASingleCommunication = match.size;
            

            # ********* RECV
            if match.blocking_recv:
                assert self.list_ranks[match.rankR].cycle <= match.endCycle, str(match.rankR) + " - cycle " + str(self.list_ranks[match.rankR].cycle) + " cycle " + str(match.endCycle);
                self.list_ranks[match.rankR].includeHaltedTime(self.list_ranks[match.rankR].cycle, match.endCycle);
                self.list_ranks[match.rankR].cycle = match.endCycle;
                if self.MQ.blockablePendingMessage[match.rankR] == 0:
                    self.list_ranks[match.rankR].state = Rank.S_NORMAL;
            else:
                self.list_ranks[match.rankR].include_iSendRecvConclusion(match.tag, match.endCycle);
            
            # Statistics
            self.list_ranks[match.rankR].amountOfCommunications = self.list_ranks[match.rankR].amountOfCommunications + 1;
            self.list_ranks[match.rankR].amountOfDataOnCommunication = self.list_ranks[match.rankR].amountOfDataOnCommunication + match.size
            if match.size > self.list_ranks[match.rankR].largestDataOnASingleCommunication:
                self.list_ranks[match.rankR].largestDataOnASingleCommunication = match.size;

            #del match;


        return END;

        

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


    def print_blank(self):
        if not self.ended:
            return None;
        biggestCycle = self.list_ranks[0].cycle;
        for ri in range(1, len(self.list_ranks)):
            if self.list_ranks[ri].cycle > biggestCycle:
                biggestCycle =  self.list_ranks[ri].cycle;
        #print(biggestCycle);
        print("biggest:"+str(biggestCycle))
        print("rank,endTime,haltedTime,numCommunications,averageMessageSize,largestData")
        for ri in range(0, len(self.list_ranks)):

            endTime = self.list_ranks[ri].cycle;
            haltedTime = self.list_ranks[ri].timeHaltedDueCommunication;
            numCommunications = self.list_ranks[ri].amountOfCommunications;
            averageCommunicationSize = self.list_ranks[ri].amountOfDataOnCommunication / numCommunications;
            largestDataOnSingleCommunication = self.list_ranks[ri].largestDataOnASingleCommunication;

            print("rank"+str(ri), end=',')
            print(str(endTime), end=',')
            print(str(haltedTime), end=',')
            print(str(numCommunications), end=',')
            print(str(averageCommunicationSize), end=',')
            print(str(largestDataOnSingleCommunication))


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
        biggestCycle = self.list_ranks[0].cycle;
        for ri in range(1, len(self.list_ranks)):
            if self.list_ranks[ri].cycle > biggestCycle:
                biggestCycle =  self.list_ranks[ri].cycle;
        #print(biggestCycle);
        print("biggest:"+str(biggestCycle))
        print("rank,endTime,haltedTime,numCommunications,averageMessageSize,largestData")
        for ri in range(0, len(self.list_ranks)):

            endTime = self.list_ranks[ri].cycle;
            haltedTime = self.list_ranks[ri].timeHaltedDueCommunication;
            numCommunications = self.list_ranks[ri].amountOfCommunications;
            averageCommunicationSize = self.list_ranks[ri].amountOfDataOnCommunication / numCommunications;
            largestDataOnSingleCommunication = self.list_ranks[ri].largestDataOnASingleCommunication;

            print("rank"+str(ri), end=',')
            print(str(endTime), end=',')
            print(str(haltedTime), end=',')
            print(str(numCommunications), end=',')
            print(str(averageCommunicationSize), end=',')
            print(str(largestDataOnSingleCommunication))