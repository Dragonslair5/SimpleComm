import configparser



class SimpleCommConfiguration:
    def __init__(self, configfile: str) -> None:
        
        config = configparser.ConfigParser();
        config.read(configfile);
        self.topology = config["TOPOLOGY"].get("topology", "SC_CC");
        
        self.CA_Allreduce = config["CollectiveAlgorithm"].get("CA_Allreduce", "reduce_bcast");
        self.CA_Alltoall = config["CollectiveAlgorithm"].get("CA_Alltoall", "basic_linear");
        self.CA_Alltoallv = config["CollectiveAlgorithm"].get("CA_Alltoallv", "nbc_like_simgrid");
        self.CA_Barrier = config["CollectiveAlgorithm"].get("CA_Barrier", "basic_linear");
        self.CA_Bcast = config["CollectiveAlgorithm"].get("CA_Bcast", "binomial_tree");
        self.CA_Reduce = config["CollectiveAlgorithm"].get("CA_reduce", "alltoroot");






def getDataTypeSize(datatype: int) -> int:
    if datatype == 0: # MPI_DOUBLE
        return 8;
    if datatype == 1: # MPI_INT
        return 4;
    if datatype == 2: # MPI_CHAR
        return 1;
    if datatype == 23: # MPI_INTEGER32
        return 4;
    assert datatype == 0, "Unknown datatype " + str(datatype)


#MPI_DATATYPE = [8, # 0 - MPI_DOUBLE
#                4, # 1 - MPI_INT
#                1  # 2 - MPI_CHAR
#                ]


# (MPIC)onstants
MPIC_SEND=0
MPIC_RECV=1

# Const TAGs for Collective Operations

MPIC_COLL_TAG_DEFAULT = 1;
MPIC_COLL_TAG_ALLREDUCE = 2;
MPIC_COLL_TAG_BARRIER = 3;
MPIC_COLL_TAG_BCAST = 4;
MPIC_COLL_TAG_REDUCE = 5;


MPIC_COLL_TAG_ALLTOALL = -1;
MPIC_COLL_TAG_ALLTOALLV = -2;


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


# send <trg> <?> <#amount> <MPI_DATATYPE>
# recv <src> <?> <#amount> <MPI_DATATYPE>
# bsend <#amount> <root> <MPI_DATATYPE>


class MQ_Match:
    def __init__(self, rankS, rankR, size, baseCycle, endCycle, tag = None, blocking_send = True, blocking_recv = True, send_origin = "", recv_origin = "", positionS = 0, positionR = 0):
        self.rankS = rankS;
        self.rankR = rankR;
        self.size = size;
        self.baseCycle = baseCycle;
        self.endCycle = endCycle;
        self.tag = tag;
        self.blocking_send = blocking_send;
        self.blocking_recv = blocking_recv;
        self.send_origin = send_origin;
        self.recv_origin = recv_origin;
        self.positionS = positionS; # Ordering
        self.positionR = positionR; # Ordering
        self.removelat = True;


    def __str__ (self):
        return "[(" + str(self.positionS) + ")S:" + str(self.rankS) + " (" + str(self.positionR) + ")R:" + str(self.rankR) + "] (base: " + str(self.baseCycle) + " end: " + str(self.endCycle) + ")"

        #print(str(self.rankS) + " -> " + str(self.rankR))
