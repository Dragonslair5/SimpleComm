
import sys

from tp_utils import *
from MPI_Constants import *
from SendRecv import *
from Col_Bcast import *
from Col_Barrier import *
from Col_Reduce import *
from Col_Allreduce import *
from Col_Alltoall import *
from Col_Alltoallv import *

class iSendRecv:

    def __init__(self, tag, endCycle):
        self.tag = tag;
        self.endCycle = endCycle;



class Rank:

    S_NORMAL=0 # Not Comunicating
    S_COMMUNICATING=2 # Is Communicating
    S_ENDED=1 # Trace has ended
    S_SR=3 # sent a SEND or RECV (unused)
    S_M=4 # has a Match, waiting for result (unused)
    S_WAITING=5; # Using WAIT or WAITALL for unblocking operations

    def __init__(self, rank, trace):
        self.rank = rank;
        self.trace = trace;
        self.index = 0;
        self.state = Rank.S_NORMAL; # start at normal
        self.cycle = 0; # CurrentCycle
        self.current_operation = "";
        
        # Non-blocking operation
        self.iSendRecvQ = [];
        self.waitingTag = None;

    #def changeState(self, newState):
    #    self.state = newState;

    #def getCurrentState(self):
    #    return self.state;

    def getCurrentStateName(self):
        if self.state == Rank.S_NORMAL:
            return "NORMAL";
        if self.state == Rank.S_COMMUNICATING:
            return "COMMUNICATING";
        if self.state == Rank.S_ENDED:
            return "ENDED";
        if self.state == Rank.S_WAITING:
            return "WAITING";
        return "Unknown";


    def include_iSendRecvConclusion(self, tag, endcycle):
        isendrecv = iSendRecv(tag, endcycle);
        self.iSendRecvQ.append(isendrecv);

    def check_iSendRecvConclusion(self, tag):
        assert self.state == Rank.S_WAITING;
        assert self.waitingTag != None;
        for i in range(len(self.iSendRecvQ)):
            if tag == self.iSendRecvQ[i].tag:
                if self.cycle < self.iSendRecvQ[i].endCycle:
                    self.cycle = self.iSendRecvQ[i].endCycle;
                del self.iSendRecvQ[i];
                self.current_operation = self.current_operation + " (iM)"
                return True; # Found completed iSendRecv
        return False; # No completed operation found with current waiting tag


    def step(self, num_ranks):
        if self.state == Rank.S_ENDED or self.state == Rank.S_COMMUNICATING:
            return None;
        if self.state == Rank.S_WAITING:
            if(self.check_iSendRecvConclusion(self.waitingTag)):
                self.state = Rank.S_NORMAL;
            return None;
        # Grab workload and increment index
        workload = self.trace[self.index];
        operation = workload[1];
        self.index = self.index + 1;

        # Consume workload
        if operation == "init":
            self.current_operation = "init-" + str(self.index);
            return None;
        if operation == "compute":
            self.current_operation = "compute-" + str(self.index);
            self.cycle = self.cycle + int(float(workload[2])); # need to float->int cause of scientific notation
            return None;
        if(operation == "send"):
            self.state = Rank.S_COMMUNICATING;
            target=int(workload[2]);
            tag = int(workload[3]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_SEND, self.rank, target, size, self.cycle, "send", tag=tag);
            self.current_operation = "send(" + str(target) + ")-" + str(self.index);
            return sr;
        if(operation == "recv"):
            self.state = Rank.S_COMMUNICATING;
            source=int(workload[2]);
            tag = int(workload[3]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_RECV, self.rank, source, size, self.cycle, "recv", tag=tag);
            self.current_operation = "recv(" + str(source) + ")-" + str(self.index);
            return sr;
        if(operation == "bcast"):
            self.current_operation = "bcast-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            root = int(workload[3]);
            datatype = getDataTypeSize(int(workload[4]));
            size = int(workload[2]) * datatype;
            bc = MQ_Bcast_entry(self.rank, root, size, self.cycle);
            return bc;
        if(operation == "barrier"):
            self.current_operation = "barrier-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            barrier = MQ_Barrier_entry(self.rank, self.cycle);
            return barrier;
        if(operation == "reduce"):
            self.current_operation = "reduce-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            root = int(workload[3]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[2]) * datatype;
            reduce = MQ_Reduce_entry(self.rank, root, size, self.cycle);
            return reduce;
        if(operation == "allreduce"):
            self.current_operation = "allreduce-"+str(self.index);
            self.state = Rank.S_COMMUNICATING;
            datatype = getDataTypeSize(int(workload[4]));
            size = int(workload[2]) * datatype;
            allreduce = MQ_Allreduce_entry(self.rank, size, self.cycle);
            return allreduce;
        if(operation == "alltoall"):
            self.current_operation = "alltoall-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            send_datatype = getDataTypeSize(int(workload[4]));
            recv_datatype = getDataTypeSize(int(workload[5]));
            send_size = int(workload[2]) * send_datatype;
            recv_size = int(workload[3]) * recv_datatype;
            alltoall = MQ_Alltoall_entry(self.rank, send_size, recv_size, self.cycle);
            return alltoall;
        if(operation == "alltoallv"): # alltoallv <send count> [send vector] <recv count> [recv vector] <recv datatype> <send datatype>
            self.current_operation = "alltoallv-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            send_datatype = getDataTypeSize(int(workload[4+2*num_ranks]));
            recv_datatype = getDataTypeSize(int(workload[5+2*num_ranks]));
            send_count = [];
            recv_count = [];
            for i in range(num_ranks):
                send_count.append(int(workload[3 + i]));
                recv_count.append(int(workload[4 + num_ranks + i]));
            alltoallv = MQ_Alltoallv_entry(self.rank, send_datatype, recv_datatype, send_count, recv_count, self.cycle);
            return alltoallv;
        if(operation == "finalize"):
            self.current_operation = "finalize-" + str(self.index);
            self.state = Rank.S_ENDED;
            return None;

        # No blocking operations
        if(operation == "isend"):
            #self.state = Rank.S_COMMUNICATING;
            target=int(workload[2]);
            tag = int(workload[3]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_SEND, self.rank, target, size, self.cycle, "isend", blocking = False, tag=tag);
            self.current_operation = "isend-(" + str(target) + ")-" + str(self.index);
            return sr;
        if(operation == "irecv"):
            #self.state = Rank.S_COMMUNICATING;
            source=int(workload[2]);
            tag = int(workload[3]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_RECV, self.rank, source, size, self.cycle, "irecv", blocking = False, tag=tag);
            self.current_operation = "irecv(" + str(source) + ")-" + str(self.index);
            return sr;
        if(operation == "wait"):
            self.current_operation = "wait-" + str(self.index);
            source = int(workload[2]);
            target = int(workload[3]);
            tag = int(workload[4]);
            self.state = Rank.S_WAITING;
            self.waitingTag = tag;
            return None;
        #if(operation == "waitall"):
        #    self.current_operation = 

        print( bcolors.FAIL + "ERROR: Unknown operation " + str(operation) + bcolors.ENDC);
        sys.exit(1);

        


        