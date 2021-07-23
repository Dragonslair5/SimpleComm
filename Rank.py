
from tp_utils import *
from MPI_Constants import *

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
    
    def step(self):
        if self.state == Rank.S_ENDED or self.state == Rank.S_COMMUNICATING:
            return None;
        # Grab workload and increment index
        workload = self.trace[self.index];
        operation = workload[1];
        self.index = self.index + 1;

        # Consume workload
        if operation == "init":
            return None;
        if operation == "compute":
            self.cycle = self.cycle + int(workload[2]);
            return None;
        if(operation == "send"):
            self.state = Rank.S_COMMUNICATING;
            target=int(workload[2]);
            datatype = MPI_DATATYPE[int(workload[5])];
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_SEND, self.rank, target, size, self.cycle);
            return sr;
        if(operation == "recv"):
            self.state = Rank.S_COMMUNICATING;
            source=int(workload[2]);
            datatype = MPI_DATATYPE[int(workload[5])];
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_RECV, self.rank, source, size, self.cycle);
            return sr;
        if(operation == "finalize"):
            self.state = Rank.S_ENDED;
            return None;

        print( bcolors.FAIL + "ERROR: Unknown operation " + str(operation) + bcolors.ENDC);
        sys.exit(1);

        


        