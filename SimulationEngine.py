
import sys

from MessageQueue import *



class SimpleCommEngine:

    def __init__(self):
        self.list_ranks = []
        self.MQ = MessageQueue()

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
        END = 0 # When END equals number of Ranks, it is the end
        
        # Step forward
        for ri in range(len(self.list_ranks)):
            sr = self.list_ranks[ri].step();
            if sr is not None:
                self.MQ.includeSendRecv(sr);
            if self.list_ranks[ri].state == Rank.S_ENDED:
                END = END + 1

        if END == len(self.list_ranks):
            return END;



        # Process MatchQueue
        match = self.MQ.processMatchQueue(self.list_ranks);
        if match is not None:
            assert self.list_ranks[match.rankS].cycle < match.endCycle;
            self.list_ranks[match.rankS].cycle = match.endCycle;
            self.list_ranks[match.rankS].state = Rank.S_NORMAL;
            assert self.list_ranks[match.rankR].cycle < match.endCycle;
            self.list_ranks[match.rankR].cycle = match.endCycle;
            self.list_ranks[match.rankR].state = Rank.S_NORMAL;

        return END;


    def showResults(self):
        print(bcolors.OKGREEN + "Result" + bcolors.ENDC)
        for ri in range(len(self.list_ranks)):
            rank = self.list_ranks[ri];
            print("{: <15}".format(rank.cycle), end='');
        print("")