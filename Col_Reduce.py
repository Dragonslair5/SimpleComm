import sys
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
        self.op_name = "reduce";
    
    def incEntry(self, reduce_entry: MQ_Reduce_entry):
        assert isinstance(reduce_entry, MQ_Reduce_entry);
        self.entries.append(reduce_entry);
        if self.baseCycle < reduce_entry.baseCycle:
            self.baseCycle = reduce_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries);


    # Based on SimGrid
    # colls::ireduce (smpi_nbc_impl.cpp)
    def process(self, algorithm: str) -> list:
        assert self.isReady();

        if(algorithm == "alltoroot"):
            return self.algorithm_allToRoot();

        print( bcolors.FAIL + "ERROR: Unknown reduce algorithm " + algorithm + bcolors.ENDC);
        sys.exit(1);



    # Based on SimGrid
    # colls::ireduce (smpi_nbc_impl.cpp)
    def algorithm_allToRoot(self) -> list:
        sr_list = [];

        for rank in range(1, self.num_ranks):
            if rank != self.root:
                # Send rank -> root
                sr = SendRecv(MPIC_SEND, rank, self.root, self.size, self.baseCycle, operation_origin=self.op_name, tag=MPIC_COLL_TAG_REDUCE);
                sr_list.append(sr);
                # Recv rank-> root
                sr = SendRecv(MPIC_RECV, self.root, rank, self.size, self.baseCycle, operation_origin=self.op_name, tag=MPIC_COLL_TAG_REDUCE);
                sr_list.append(sr);

        return sr_list;
