
import sys

from MessageQueue import *





class SimpleCommEngine:

    def __init__(self, nRanks, configfile: str,  verbose = True):
        self.list_ranks = []
        self.saveState = [0] * nRanks;
        self.nSteps = 0;
        self.verbose = verbose;
        self.ended = False;
        self.config : SimpleCommConfiguration;
        self.config = SimpleCommConfiguration(configfile);
        self.MQ : MessageQueue;
        self.MQ = MessageQueue(nRanks, self.config)


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
                #assert self.list_ranks[match.rankS].cycle < match.endCycle, str(match.rankS) + " - cycle " + str(self.list_ranks[match.rankS].cycle) + " cycle " + str(match.endCycle);
                self.list_ranks[match.rankS].cycle = match.endCycle;
                if self.MQ.blockablePendingMessage[match.rankS] == 0:
                    self.list_ranks[match.rankS].state = Rank.S_NORMAL;
            else:
                self.list_ranks[match.rankS].include_iSendRecvConclusion(match.tag, match.endCycle);
            # ********* RECV
            if match.blocking_recv:
                #assert self.list_ranks[match.rankR].cycle < match.endCycle, str(match.rankR) + " - cycle " + str(self.list_ranks[match.rankR].cycle) + " cycle " + str(match.endCycle);
                self.list_ranks[match.rankR].cycle = match.endCycle;
                if self.MQ.blockablePendingMessage[match.rankR] == 0:
                    self.list_ranks[match.rankR].state = Rank.S_NORMAL;
            else:
                self.list_ranks[match.rankR].include_iSendRecvConclusion(match.tag, match.endCycle);
            #del match;


        return END;


    def showResults(self):
        if self.verbose:
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
            input("")
        elif self.ended:
            biggestCycle = self.list_ranks[0].cycle;
            for ri in range(1, len(self.list_ranks)):
                if self.list_ranks[ri].cycle > biggestCycle:
                    biggestCycle =  self.list_ranks[ri].cycle;
            print(biggestCycle);
        


