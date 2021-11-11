import configparser



class SimpleCommConfiguration:
    def __init__(self, configfile: str) -> None:
        
        config = configparser.ConfigParser();
        config.read(configfile);
        self.topology = config["TOPOLOGY"].get("topology", "SC_CC");
        self.internode_bandwidth = float(config["TOPOLOGY"].get("internode_bandwidth", "10"));
        self.internode_latency = float(config["TOPOLOGY"].get("internode_latency", "10"));
        self.intranode_bandwidth = float(config["TOPOLOGY"].get("intranode_bandwidth", "10"));
        self.intranode_latency = float(config["TOPOLOGY"].get("intranode_latency", "10"));
        
        
        # NOTE There might be a better way to grab a boolean
        self.computation : bool
        self.computation = config["TOPOLOGY"].get("computation", "True");
        #print(self.computation)
        if self.computation == "True":
            self.computation = True;
        else:
            self.computation = False;
        self.processing_speed = config["TOPOLOGY"].getint("processing_speed", "1")
        


        # Collective Algorithms
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
    if datatype == 43: # MPI_DOUBLE_PRECISION (DOUBLE PRECISION Fortran)
        return 16; # MPI_COMPLEX or MPI_DOUBLE_COMPLEX? (Fortran)
        return 8; # 8 or 16? Was not working with 8
    assert datatype == 0, "Unknown datatype " + str(datatype)


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

# These are the possible actions from SimGrid trace, but not all of them are implemented (See on Rank.py which ones are implemented)
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


class MQ_Match:
    def __init__(self, id, rankS, rankR, size, baseCycle, endCycle, tag = None, blocking_send = True, blocking_recv = True, send_origin = "", recv_origin = "", positionS = 0, positionR = 0, latency = 0, col_id = 0):
        self.rankS = rankS;
        self.rankR = rankR;
        self.size = size;
        self.baseCycle = baseCycle;
        self.endCycle = endCycle;
        self.tag = tag;
        self.id = id;

        self.solvedCycle = -1; # This demarks a portion of the connection that has been shared (for SHARED channel execution) [-1 for unused]
        self.bw_factor = 1; # This is the factor of the sharing portion demarked by solvedCycle (for SHARED channel execution)
        self.latency = latency;
        #self.incLatency = True
        self.col_id = col_id;
        

        self.blocking_send = blocking_send;
        self.blocking_recv = blocking_recv;
        
        self.send_origin = send_origin; # The operations name
        self.recv_origin = recv_origin; # The operations name

        # Ordering for when it matters (TAG not negative)
        self.positionS = positionS; # Ordering
        self.positionR = positionR; # Ordering
        
        # Miscelaneous
        self.removelat = True;
        self.fused = False;

    def getUpperCycle(self) -> float:
        assert self.solvedCycle <= self.endCycle, "solvedCycle cannot be higher than endCycle";
        if self.solvedCycle != -1:
            return self.solvedCycle;
        else:
            return self.endCycle;


    def __str__ (self):
        return "[(" + str(self.positionS) + ")S:" + str(self.rankS) + " (" + str(self.positionR) + ")R:" + str(self.rankR) + "] (base: " + str(self.baseCycle) + " end: " + str(self.endCycle) + ")" + " lat: " + str(self.latency) +  " ID: " + str(self.id) + " col_id: " + str(self.col_id)

        #print(str(self.rankS) + " -> " + str(self.rankR))
