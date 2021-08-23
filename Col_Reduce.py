from tp_utils import *
from MPI_Constants import *
from SendRecv import *

class MQ_Reduce_entry:
    def __init__ (self, rank, root, size, baseCycle):
        self.rank = rank;
        self.root = root;
        self.size = size;
        self.baseCycle = baseCycle;

class MQ_Reduce:
    def __init__ (self, num_ranks, root, size):
        self.num_ranks = num_ranks;
        self.entries = [];
        self.root = root;
        self.size = size;
        self.baseCycle = 0;
        self.op_name = "bcast"
    
    def incEntry(self, reduce_entry: MQ_Reduce_entry):
        assert isinstance(reduce_entry, MQ_Reduce_entry);
        self.entries.append(reduce_entry);
        if self.baseCycle < reduce_entry.baseCycle:
            self.baseCycle = reduce_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries);


    # Based on SimGrid
    # colls::ireduce (smpi_nbc_impl.cpp)
    def process(self):
        assert self.isReady();
        sr_list = [];

        for rank in range(1, self.num_ranks):
            if rank != self.root:
                # Send rank -> root
                sr = SendRecv(MPIC_SEND, rank, self.root, self.size, self.baseCycle);
                sr_list.append(sr);
                # Recv rank-> root
                sr = SendRecv(MPIC_RECV, self.root, rank, self.size, self.baseCycle);
                sr_list.append(sr);

        return sr_list;