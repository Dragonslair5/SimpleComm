


MPI_DATATYPE = [8, # 0 - MPI_DOUBLE
                4, # 1 - MPI_INT
                1  # 2 - MPI_CHAR
                ]


MPIC_SEND=0
MPIC_RECV=1

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
    def __init__(self, kind, rank, partner, size, baseCycle):
        self.kind=kind; # SEND or RECV
        self.rank = rank;
        self.partner = partner;
        self.size = size;
        self.baseCycle = baseCycle;

class MQ_Match:
    def __init__(self, rankS, rankR, size, baseCycle, endCycle):
        self.rankS = rankS;
        self.rankR = rankR;
        self.size = size;
        self.baseCycle = baseCycle;
        self.endCycle = endCycle;