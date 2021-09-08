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
        self.baseCycle = 0;
        self.op_name = "barrier";
    
    # TODO: Maybe we do not need a baseCycle here? use baseCycle of each entry instead?
    def incEntry(self, barrier_entry: MQ_Barrier_entry):
        assert isinstance(barrier_entry, MQ_Barrier_entry);
        self.entries.append(barrier_entry);
        if self.baseCycle < barrier_entry.baseCycle:
            self.baseCycle = barrier_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries);

    # Based on SimGrid
    # barrier__ompi_basic_linear (barrier-ompi.cpp)
    def process(self):
        assert self.isReady();
        sr_list = []

        for rank in range(1, self.num_ranks):
            # Current rank to rank 0
            sr = SendRecv(MPIC_SEND, rank, 0, 1, self.baseCycle, operation_origin=self.op_name, tag=MPIC_COLL_TAG_BARRIER);
            sr_list.append(sr);
            sr = SendRecv(MPIC_RECV, 0, rank, 1, self.baseCycle, operation_origin=self.op_name, tag=MPIC_COLL_TAG_BARRIER);
            sr_list.append(sr);

        for rank in range(1, self.num_ranks):
            # Rank 0 to current rank
            sr = SendRecv(MPIC_SEND, 0, rank, 1, self.baseCycle, operation_origin=self.op_name, tag=MPIC_COLL_TAG_BARRIER);
            sr_list.append(sr);
            sr = SendRecv(MPIC_RECV, rank, 0, 1, self.baseCycle, operation_origin=self.op_name, tag=MPIC_COLL_TAG_BARRIER);
            sr_list.append(sr);
        return sr_list;