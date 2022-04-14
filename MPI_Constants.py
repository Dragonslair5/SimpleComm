import configparser
from os.path import exists
import sys
from tp_utils import *
from math import *

class SimpleCommConfiguration:
    def __init__(self, configfile: str) -> None:
        
        if not exists(configfile):
            print( bcolors.FAIL + "Configuration file does not exist" + bcolors.ENDC);
            sys.exit(1);



        config = configparser.ConfigParser();
        config.read(configfile);

        # NOTE There might be a better way to grab a boolean
        self.computation : bool
        self.computation = config["Simulation"].get("computation", "True");
        if self.computation == "True":
            self.computation = True;
        else:
            self.computation = False;
        self.show_progress_level = config["Simulation"].get("show_progress_level", "blank");
        self.print_communication_trace = config["Simulation"].get("print_communication_trace", "True");
        if self.print_communication_trace == "True":
            self.print_communication_trace = True;
        else:
            self.print_communication_trace = False;


        self.topology = config["TOPOLOGY"].get("topology", "KAHUNA");

        self.internode_bandwidth = float(config["TOPOLOGY"].get("internode_bandwidth", "10"));
        self.internode_latency = float(config["TOPOLOGY"].get("internode_latency", "10"));
        self.intranode_bandwidth = float(config["TOPOLOGY"].get("intranode_bandwidth", "10"));
        self.intranode_latency = float(config["TOPOLOGY"].get("intranode_latency", "10"));
        
        self.fmu_bandwidth = float(config["TOPOLOGY"].get("fmu_bandwidth", "10"));
        self.fmu_latency = float(config["TOPOLOGY"].get("fmu_latency", "10"));
        self.fmu_pivot_value = float(config["TOPOLOGY"].get("fmu_pivot_value", "10"));

        # SimGrid               (65536 bytes) (64KB in short)
        # OpenMPI Version 4.0.5 (65536 bytes) (64KB in short) (btl_tcp_component.c)
        # MPICH2 Version 3.3.1  (262144 bytes) (256KB in short) (mpidi_ch3_post.h)
        self.eager_protocol_max_size = int(config["TOPOLOGY"].get("eager_protocol_max_size", "0")); # 0 means to turn it off

        self.number_of_FMUs = int(config["TOPOLOGY"].get("number_of_fmus", "10"));

        self.processing_speed = config["TOPOLOGY"].getint("processing_speed", "1");
        


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




class MPI_Operations:

    # P2P    
    MPI_SEND = 0;
    MPI_RECV = 1;
    MPI_ISEND=2;
    MPI_IRECV=3;
    MPI_WAIT=4;
    MPI_WAITALL=5;


    # Collectives
    MPI_BCAST=100;
    MPI_BARRIER=101;
    MPI_REDUCE=102;
    MPI_ALLREDUCE=103;
    MPI_ALLTOALL=104;
    MPI_ALLTOALLV=105;




    @staticmethod
    def getOperationNameByID(ID: int)->str:
        
        if ID == MPI_Operations.MPI_SEND:
            return "MPI_SEND";
        if ID == MPI_Operations.MPI_RECV:
            return "MPI_RECV";
        if ID == MPI_Operations.MPI_ISEND:
            return "MPI_ISEND";
        if ID == MPI_Operations.MPI_IRECV:
            return "MPI_IRECV";
        if ID == MPI_Operations.MPI_WAIT:
            return "MPI_WAIT";
        if ID == MPI_Operations.MPI_WAITALL:
            return "MPI_WAITALL";


        if ID == MPI_Operations.MPI_BCAST:
            return "MPI_BCAST";
        if ID == MPI_Operations.MPI_BARRIER:
            return "MPI_BARRIER";
        if ID == MPI_Operations.MPI_REDUCE:
            return "MPI_REDUCE";
        if ID == MPI_Operations.MPI_ALLREDUCE:
            return "MPI_ALLREDUCE";
        if ID == MPI_Operations.MPI_ALLTOALL:
            return "MPI_ALLTOALL";
        if ID == MPI_Operations.MPI_ALLTOALLV:
            return "MPI_ALLTOALLV";

        print( bcolors.FAIL + "ERROR: Unknown Operation ID: " + str(ID) + bcolors.ENDC);
        sys.exit(1);






class MQ_Match:
    def __init__(self, 
                 id, 
                 rankS, 
                 rankR, 
                 size, 
                 baseCycle, 
                 endCycle, 
                 tag = None, 
                 blocking_send = True, 
                 blocking_recv = True, 
                 send_origin = "", 
                 send_operation_ID = MPI_Operations.MPI_SEND,
                 recv_origin = "", 
                 recv_operation_ID = MPI_Operations.MPI_RECV,
                 positionS = 0, 
                 positionR = 0, 
                 bandwidth = 1,
                 latency = 0, 
                 col_id = 0):



        self.rankS = rankS;
        self.rankR = rankR;

        self.size = size;
        self.baseCycle = baseCycle;



        # Individual timings for SEND/RECV
        self.send_original_baseCycle = -1;
        self.send_baseCycle = -1; # >= baseCycle
        self.send_endCycle = -1; # <= endCycle
        self.recv_original_baseCycle = -1;
        self.recv_baseCycle = -1; # >= baseCycle
        self.recv_endCycle = -1; # <= endCycle
        
        self.initialized = False;
        self.still_solving_send = True;
        #****

        self.endCycle = endCycle;
        self.tag = tag;
        self.id = id;

        self.solvedCycle = -1; # This demarks a portion of the connection that has been shared (for SHARED channel execution) [-1 for unused]
        self.bw_factor = 1; # This is the factor of the sharing portion demarked by solvedCycle (for SHARED channel execution)
        self.bw = bandwidth;
        self.latency = latency;
        #self.incLatency = True
        self.col_id = col_id;
        self.original_baseCycle = baseCycle # For debugging
        self.transmitted_data = [] # For debugging
        self.data_sent = 0
        

        self.blocking_send = blocking_send;
        self.blocking_recv = blocking_recv;
        
        self.send_origin = send_origin; # The operations name
        self.send_operation_ID = send_operation_ID; # The operation ID
        self.recv_origin = recv_origin; # The operations name
        self.recv_operation_ID = recv_operation_ID; # The operation ID

        # Ordering for when it matters (TAG not negative)
        self.positionS = positionS; # Ordering
        self.positionR = positionR; # Ordering
        

    

    def sep_initializeMatch(self, transmissionTime: float):
        assert self.send_baseCycle >= 0, "This should have been initialized"
        assert self.recv_baseCycle >= 0, "This should have been initialized"
        assert self.initialized == False, "Cant initialize an already initialized match"
        latency = self.latency;
        self.send_endCycle = self.send_baseCycle + latency + transmissionTime;
        self.recv_endCycle = self.recv_baseCycle + latency + transmissionTime;
        self.initialized = True;

    
    def sep_getBaseCycle(self) -> float:
        if self.still_solving_send:
            return self.send_baseCycle;
        else:
            return self.recv_baseCycle;

    def sep_getEndCycle(self) -> float:
        if self.still_solving_send:
            return self.send_endCycle;
        else:
            return self.recv_endCycle;
    
    def sep_incrementCycle(self, increment: float):
        if self.still_solving_send:
            self.send_baseCycle = self.send_baseCycle + increment;
            self.send_endCycle = self.send_endCycle + increment;
        else:
            self.recv_baseCycle = self.recv_baseCycle + increment;
            self.recv_endCycle = self.recv_endCycle + increment;
    
    def sep_move_RECV_after_SEND(self):
        assert self.still_solving_send == True, "Wtf?"
        minToStart = self.send_endCycle;
        inc = minToStart - self.recv_baseCycle;

        if inc > 0:
            self.recv_baseCycle = self.recv_baseCycle + inc;
            self.recv_endCycle = self.recv_endCycle + inc;
        self.still_solving_send = False;
        




    
    def getUpperCycle(self) -> float:
        assert self.solvedCycle <= self.endCycle, "solvedCycle cannot be higher than endCycle";
        if self.solvedCycle != -1:
            return self.solvedCycle;
        else:
            return self.endCycle;

    def includeTransmittedData(self, length, bw_factor, data_size):
        self.transmitted_data.append([length, bw_factor, data_size]);
        #print(str(data_size) + " / " + str(self.size + 16))
        self.data_sent = self.data_sent + trunc(data_size);
        #print("<" + str(self.id) + "> " + str(self.data_sent) + "/" +str(self.size+16))   
        #print(self.transmitted_data)
        #print(self)
        #print("Truncated: " + str(trunc(self.data_sent)) + "  Raw: " + str(self.data_sent))
        assert trunc(self.data_sent) <= (self.size+16), str(trunc(self.data_sent)) + " > " + str(self.size + 16)
        #assert self.data_sent <= (self.size+16), str(self.data_sent) + " > " + str(self.size + 16)

    @DeprecationWarning
    def checkCorrectness(self):
        size = 0
        sending = 0;
        willsend = 0
        bw = 2500000000
        if self.solvedCycle != -1:
            size = size + (self.endCycle - self.solvedCycle) * bw
            sending = (self.endCycle - self.solvedCycle) * bw;
            size = size + (self.solvedCycle - self.baseCycle) * (bw/self.bw_factor)
            willsend = (self.solvedCycle - self.baseCycle) * (bw/self.bw_factor)
        else:
            size = size + (self.endCycle - self.baseCycle) * (bw/self.bw_factor)
            willsend = (self.endCycle - self.baseCycle) * (bw/self.bw_factor)
        size = size + self.data_sent;

        print("ID: " + str(self.id) + " size: " + str(self.size+16) + " sent: " + str(round(self.data_sent)) + " sending: " + str(round(sending)) + " willsend: " + str(round(willsend)) + " data: " + str(round(size)))

    def __str__ (self):
#        return "[(" + str(self.positionS) + ")S:" + str(self.rankS) + " (" + str(self.positionR) + ")R:" + str(self.rankR) + "] (base: " + str(self.baseCycle) + " solved: " + str(self.solvedCycle) + " end: " + str(self.endCycle) + ")" + " lat: " + str(self.latency) +  " ID: " + str(self.id) + " col_id: " + str(self.col_id) + " bw_factor: " + str(self.bw_factor) + " originalBaseCycle: " + str(self.original_baseCycle) +" duration: " + str(self.endCycle - self.original_baseCycle) + " size: " + str(self.size+16) + " BW: " + str((self.size+16)/(self.endCycle - self.original_baseCycle))
        data_sent = 0;
        for i in range(0, len(self.transmitted_data)):
            data_sent = data_sent + self.transmitted_data[i][2];
        return "[(" + str(self.positionS) + ")S:" + str(self.rankS) + " (" + str(self.positionR) + ")R:" + str(self.rankR) + "] (base: " + str(self.baseCycle) + " solved: " + str(self.solvedCycle) + " end: " + str(self.endCycle) + ")" + " lat: " + str(self.latency) +  " ID: " + str(self.id) + " col_id: " + str(self.col_id) + " bw_factor: " + str(self.bw_factor) + " originalBaseCycle: " + str(self.original_baseCycle) +" duration: " + str(self.endCycle - self.original_baseCycle) + " size: " + str(self.size+16) + " BW: " + str((self.size+16)/(self.endCycle - self.original_baseCycle)) + " sent: " + str(data_sent)
        #print(str(self.rankS) + " -> " + str(self.rankR))

    def __strx__(self):

        return "S:" + str(self.rankS) + ": " + str(self.send_endCycle) + "    R:" + str(self.rankR) + ": " + str(self.recv_endCycle)
