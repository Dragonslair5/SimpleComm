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


def iterate():
    print("a");

def communicate(workload):
    workload = int(workload)
    return latency + workload/bandwidth;
    return 0;
    return int(workload);

def consume(workload):
    workload_length = len(workload);
    # <Rank> <Operation>
    operation = workload[1];
    print("Rank-" + str(workload[0]) + " operation = " + operation, end='');
    if(operation == 'init' or operation == 'finalize'):
        #print("Rank-" + str(workload[0] + " operation = ") 
        print("");
        return 0;
    if(operation == "compute"):
        # <#workload>
        print(" W = " + workload[2])
        return int(workload[2]);
    if(operation == "send"):
        # <target> <?> <#amount> <#datatype>
        target=int(workload[2])
        datatype = MPI_DATATYPE[int(workload[5])];
        size = int(workload[4]) * datatype;
        print(" T = " + str(target) + " size = " + str(size) );
        return communicate(size);
    if(operation == "recv"):
        # <target> <?> <#amount> <#datatype>
        source=int(workload[2])
        datatype = MPI_DATATYPE[int(workload[5])];
        size = int(workload[4]) * datatype;
        print(" S = " + str(source) + " size = " + str(size) );
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
    # The index of each rank (where we are at the given trace)
    # ***********************
    rank_index = [0] * nRanks

    # Main Loop
    while True:
        actions_taken = nRanks;
        for ri in range(0, nRanks, 1):
            currentIndex = rank_index[ri];
            if(rank_index[ri] < len(list_of_traces[ri])):
                workload = list_of_traces[ri][rank_index[ri]]
                inc = consume(workload);
                cycles[ri] = cycles[ri] + inc;
                print("Rank-" + str(ri) + " inc " + str(inc) + " cycles" );
                rank_index[ri] = rank_index[ri] + 1;
            else:
                actions_taken = actions_taken - 1;
        if actions_taken == 0:
            break;




    print(bcolors.OKGREEN + "Result" +bcolors.ENDC)
    print(cycles)


    #print(list_of_traces);



if __name__ == "__main__":
    main()


