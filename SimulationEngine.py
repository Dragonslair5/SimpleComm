
import sys

from MessageQueue import *



class SimpleCommEngine:

    def __init__(self, nRanks):
        self.list_ranks = []
        self.MQ = MessageQueue(nRanks)
        self.saveState = [0] * nRanks;
        self.nSteps = 0;

    def read_traces(self, nRanks, traces_path):

        for rank in range(1, nRanks+1, 1):
            trace = [];
            rankFile = open(traces_path+"/"+"rank-"+str(rank)+".txt", "r");
            for line in rankFile:
                stripped_line = line.strip();
                line_list = stripped_line.split();
                trace.append(line_list);
            rankFile.close();

            print( bcolors.OKBLUE + "Rank-" + str(rank) + bcolors.ENDC);
            print(trace);

            aux_rank = Rank(rank-1, trace);

            self.list_ranks.append(aux_rank);



    def simulate(self):
        self.nSteps = self.nSteps + 1;
        END = 0 # When END equals number of Ranks, it is the end
        
        # Step forward
        for ri in range(len(self.list_ranks)):
            # Try to progress on simulation (step)
            operation = self.list_ranks[ri].step();
            if operation is not None:
                if isinstance(operation, SendRecv):
                    self.MQ.includeSendRecv(operation);
                elif isinstance(operation, MQ_bcast_entry):
                    self.MQ.include_Bcast(operation, len(self.list_ranks));
            if self.list_ranks[ri].state == Rank.S_ENDED:
                END = END + 1

        if END == len(self.list_ranks):
            return END;

        # Process Collective Operations
        self.MQ.processCollectiveOperations();

        # Process MatchQueue
        match = self.MQ.processMatchQueue(self.list_ranks);
        if match is not None:
            #print(" SR " + str(match.rankS) + " --> " + str(match.rankR))
            assert self.list_ranks[match.rankS].cycle < match.endCycle;
            self.list_ranks[match.rankS].cycle = match.endCycle;
            if self.MQ.blockablePendingMessage[match.rankS] == 0:
                self.list_ranks[match.rankS].state = Rank.S_NORMAL;
            assert self.list_ranks[match.rankR].cycle < match.endCycle, "cycle " + str(self.list_ranks[match.rankR].cycle) + " cycle " + str(match.endCycle);
            self.list_ranks[match.rankR].cycle = match.endCycle;
            if self.MQ.blockablePendingMessage[match.rankR] == 0:
                self.list_ranks[match.rankR].state = Rank.S_NORMAL;

        return END;


    def showResults(self):
        print(bcolors.OKGREEN + "Result - step " + str(self.nSteps) + bcolors.ENDC)
        for ri in range(len(self.list_ranks)):
            rank = self.list_ranks[ri];
            if self.saveState[ri] != rank.cycle:
                self.saveState[ri] = rank.cycle;
                print(bcolors.OKCYAN, end='');
            print("{: <15}".format(rank.cycle), end='');
            print(bcolors.ENDC, end='');
        print("")

