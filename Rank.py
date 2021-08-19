
import sys
from Col_Allreduce import MQ_Allreduce_entry

from tp_utils import *
from MPI_Constants import *
from SendRecv import *
from Col_Bcast import *
from Col_Barrier import *
from Col_Reduce import *
from Col_Allreduce import *

class Rank:

    S_NORMAL=0 # Not Comunicating
    S_COMMUNICATING=2 # Is Communicating
    S_ENDED=1 # Trace has ended
    S_SR=3 # sent a SEND or RECV
    S_M=4 # has a Match, waiting for result

    def __init__(self, rank, trace):
        self.rank = rank;
        self.trace = trace;
        self.index = 0;
        self.state = Rank.S_NORMAL; # start at normal
        self.cycle = 0; # CurrentCycle
        self.current_operation = ""
    
    def step(self):
        if self.state == Rank.S_ENDED or self.state == Rank.S_COMMUNICATING:
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
            #self.cycle = self.cycle + int(float(workload[2])); # need to float->int cause of scientific notation
            return None;
        if(operation == "send"):
            self.current_operation = "send-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            target=int(workload[2]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_SEND, self.rank, target, size, self.cycle, "send");
            return sr;
        if(operation == "recv"):
            self.current_operation = "recv-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            source=int(workload[2]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_RECV, self.rank, source, size, self.cycle, "recv");
            return sr;
        if(operation == "bcast"):
            self.current_operation = "bcast-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            root = int(workload[3])
            datatype = getDataTypeSize(int(workload[4]))
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
        if(operation == "finalize"):
            self.current_operation = "finalize-" + str(self.index);
            self.state = Rank.S_ENDED;
            return None;

        print( bcolors.FAIL + "ERROR: Unknown operation " + str(operation) + bcolors.ENDC);
        sys.exit(1);

        


        