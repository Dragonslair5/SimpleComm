#! /usr/bin/python3

import sys

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

MPI_DATATYPE = [8, # 0 - MPI_DOUBLE
                4, # 1 - MPI_INT
                1  # 2 - MPI_CHAR
                ] 




# *****************************
# Global Defined values
SEND=0
RECV=1


# Control
lowestCycle=0
lowestCycleIndex=0
highestCycle=0

# Links
linkLastCycle=0;

# Constants


#Dunmmy values
latency = 1
bandwidth = 1
# Using the model T = L + S/BW
# *****************************

actions = ["init",
"finalize",
"comm_size",
"comm_split",
"comm_dup",
"send",
"isend",
"recv",
"irecv",
"test",
"wait",
"waitall",
"barrier",
"bcast",
"reduce",
"allreduce",
"alltoall",
"alltoallv",
"gather",
"scatter",
"gatherv",
"scatterv",
"allgather",
"allgatherv",
"reducescatter",
"compute",
"sleep",
"location"]



class SendRecv:
    def __init__(self, kind, rank, partner, size):
        self.kind=kind; # SEND or RECV
        self.rank = rank;
        self.partner = partner;
        self.size = size;
        self.baseCycle;

class MQ_Match:
    def __init__(self, rankS, rankR, size, baseCycle):
        self.rankS = rankS;
        self.rankR = rankR;
        self.size = size;
        self.baseCycle = baseCycle;

#class MPI_Operation:


'''
class MPI_Send:
    def __init__(self, rank, target, size):
        self.rank = rank;
        self.target = target;
        self.size = size;

class MPI_Recv:
    def __init__(self, source, size):
        self.rank = rank;
        self.source = source;
        self.size = size;
'''

class MessageQueue:
    def __init__(self):
        sendQ = []
        recvQ = []
        matchQ = []

    
    def includeSendRecv(self, sendrecv: SendRecv):
        if sendrecv.kind == SEND:
            if not self.checkMatch(sendrecv):
                self.sendQ.append(sendrecv)
        elif sendrecv.kind == RECV:
            if not self.checkMatch(sendrecv):
                self.recvQ.append(sendrecv)
        else:
            print( bcolors.FAIL + "ERROR: Unknown SendRecv " + str(operation) + bcolors.ENDC);
            sys.exit(1);
            

    def checkMatch(self, sendrecv: SendRecv):
        if sendrecv.kind == SEND:
            partner_queue = self.recvQ;
            for i in len(partner_queue):
                if ( partner_queue[i].partner == sendrecv.rank and 
                    sendrecv.partner == partner_queue[i].rank and
                    sendrecv.size == partner_queue[i].size ):
                    partner = partner_queue.pop(i);
                    if sendrecv.baseCycle > partner.baseCycle:
                        baseCycle = sendrecv.baseCycle;
                    else:
                        baseCycle = partnet.baseCycle;
                    matchQ.append(MQ_Match(sendrecv.rank, partner.rank, partner.size, baseCycle));
                    return True; # Match!
            return False; # Not a Match!

        elif sendrecv.kind == RECV:
            partner_queue = self.sendQ;
            for i in len(partner_queue):
                if ( partner_queue[i].partner == sendrecv.rank and 
                    sendrecv.partner == partner_queue[i].rank and
                    sendrecv.size == partner_queue[i].size ):
                    partner = partner_queue.pop(i);
                    if sendrecv.baseCycle > partner.baseCycle:
                        baseCycle = sendrecv.baseCycle;
                    else:
                        baseCycle = partnet.baseCycle;
                    matchQ.append(MQ_Match(partner.rank, sendrecv.rank, partner.size, baseCycle));
                    return True; # Match!
            return False; # Not a Match!
        else:
            print( bcolors.FAIL + "ERROR: Unknown SendRecv " + str(operation) + bcolors.ENDC);
            sys.exit(1);

        
                 



def iterate():
    print("a");

def communicate(workload):
    workload = int(workload)
    return latency + workload/bandwidth;
    return 0;
    return int(workload);

def consume(workload, basecycle):
    workload_length = len(workload);
    # <Rank> <Operation>
    operation = workload[1];
    #print("Rank-" + str(workload[0]) + " operation = " + operation, end='');
    if(operation == 'init' or operation == 'finalize'):
        #print("Rank-" + str(workload[0] + " operation = ") 
    #    print("");
        return 0;
    if(operation == "compute"):
        # <#workload>
    #    print(" W = " + workload[2])
        return int(workload[2]);
    if(operation == "send"):
        # <target> <?> <#amount> <#datatype>
        target=int(workload[2])
        datatype = MPI_DATATYPE[int(workload[5])];
        size = int(workload[4]) * datatype;
    #    print(" T = " + str(target) + " size = " + str(size) );
        return communicate(size);
    if(operation == "recv"):
        # <target> <?> <#amount> <#datatype>
        source=int(workload[2])
        datatype = MPI_DATATYPE[int(workload[5])];
        size = int(workload[4]) * datatype;
    #    print(" S = " + str(source) + " size = " + str(size) );
        return communicate(size);
    print( bcolors.FAIL + "ERROR: Unknown operation " + str(operation) + bcolors.ENDC);
    sys.exit(1);
    #return 0;


def main():

    if len (sys.argv) < 3:
        print("Usage: python3 simple_comm.py <#ranks> <path for traces>");
        sys.exit(1);


    nRanks=int(sys.argv[1]);
    files_path=sys.argv[2];

    

    # Building up the traces
    # **********************
    print(str(nRanks) + " on " + files_path);
    
    list_of_traces = [];

    for rank in range(1, nRanks+1, 1):
        trace = [];
        rankFile = open(files_path+"/"+"rank-"+str(rank)+".txt", "r");
        for line in rankFile:
            stripped_line = line.strip();
            line_list = stripped_line.split();
            trace.append(line_list);
        rankFile.close();

        print( bcolors.OKBLUE + "Rank-" + str(rank) + bcolors.ENDC);
        print(trace);

        list_of_traces.append(trace);


    # The cycles of each rank
    # ***********************
    cycles = [0] * nRanks
    finished = [0] * nRanks;
    # The index of each rank (where we are at the given trace)
    # ***********************
    rank_index = [0] * nRanks

    for ri in range(0, nRanks, 1):
        print("{: <15}".format(  "Rank-"+str(ri)  ), end='');
    print("");


    # Main Loop
    while True:
        actions_taken = nRanks;
        for ri in range(0, nRanks, 1):
            currentIndex = rank_index[ri];
            if(rank_index[ri] < len(list_of_traces[ri])):
                workload = list_of_traces[ri][rank_index[ri]]
                inc = consume(workload, cycles[ri]);
                cycles[ri] = cycles[ri] + inc;
                #print("Rank-" + str(ri) + " inc " + str(inc) + " cycles" );
                rank_index[ri] = rank_index[ri] + 1;
            else:
                finished[ri] = 1;
                actions_taken = actions_taken - 1;
            #print("{: <15}".format(  cycles[ri]  ), end='');
        #print("");

        # Everyone already finished?
        if actions_taken == 0:
            break;
        
        # Printing cycles for debugging
        aux_index = 0;
        for ri in range(0, nRanks, 1):
            if finished[ri] == 0:
                lowestCycle = cycles[ri];
                lowestCycleIndex = ri;
                aux_index=ri;
                break;
        for ri in range(aux_index, nRanks, 1):
            if lowestCycle > cycles[ri] and finished[ri] == 0:
                lowestCycle = cycles[ri];
                lowestCycleIndex = ri;
        for ri in range(0, nRanks, 1):
            if cycles[ri] == lowestCycle:
                if finished[ri] == 0:
                    print(bcolors.OKCYAN + "{: <15}".format(  cycles[ri]  ) + bcolors.ENDC, end='');
                else:
                    print("{: <15}".format(""), end='');
            else:
                if finished[ri] == 0:
                    print("{: <15}".format(  cycles[ri]  ), end='');
                else:
                    print("{: <15}".format(""), end='');
        print("")
        # *****************************


    print(bcolors.OKGREEN + "Result" +bcolors.ENDC)
    print(cycles)


    #print(list_of_traces);



if __name__ == "__main__":
    main()


