import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *

class MQ_Barrier_entry:
    def __init__ (self, rank, baseCycle):
        self.rank = rank;
        self.baseCycle = baseCycle;

class MQ_Barrier:
    def __init__ (self, num_ranks):
        self.num_ranks = num_ranks;
        self.entries = [];
        #self.baseCycle = 0;
        self.baseCycle = [0] * num_ranks;
        self.op_name = "barrier";
    
    # TODO: Maybe we do not need a baseCycle here? use baseCycle of each entry instead?
    def incEntry(self, barrier_entry: MQ_Barrier_entry):
        assert isinstance(barrier_entry, MQ_Barrier_entry);
        self.entries.append(barrier_entry);
        #if self.baseCycle < barrier_entry.baseCycle:
        #    self.baseCycle = barrier_entry.baseCycle;
        self.baseCycle[barrier_entry.rank] = barrier_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries);

    # Based on SimGrid
    # barrier__ompi_basic_linear (barrier-ompi.cpp)
    def process(self, algorithm: str) -> list:
        assert self.isReady();

        if (algorithm == "basic_linear"):
            return self.algorithm_basic_linear();
        
        print( bcolors.FAIL + "ERROR: Unknown Barrier algorithm " + algorithm + bcolors.ENDC);
        sys.exit(1);
        

    def algorithm_basic_linear(self)->list:
        sr_list = []

        for rank in range(1, self.num_ranks):
            # Current rank to rank 0
            # (simgrid) send
            sr = SendRecv(MPIC_SEND, rank, 0, 0, self.baseCycle[rank], operation_origin=self.op_name, tag=MPIC_COLL_TAG_BARRIER, blocking=True, col_id=1);
            sr_list.append(sr);
            # (simgrid) irecv
            sr = SendRecv(MPIC_RECV, 0, rank, 0, self.baseCycle[0], operation_origin=self.op_name, tag=MPIC_COLL_TAG_BARRIER, blocking=True, col_id=1);
            sr_list.append(sr);

        for rank in range(1, self.num_ranks):
            # Rank 0 to current rank
            # (simgrid) isend
            sr = SendRecv(MPIC_SEND, 0, rank, 0, self.baseCycle[0], operation_origin=self.op_name, tag=MPIC_COLL_TAG_BARRIER, blocking=True, col_id=2);
            sr_list.append(sr);
            # (simgrid) recv
            sr = SendRecv(MPIC_RECV, rank, 0, 0, self.baseCycle[rank], operation_origin=self.op_name, tag=MPIC_COLL_TAG_BARRIER, blocking=True, col_id=2);
            sr_list.append(sr);
        return sr_list;