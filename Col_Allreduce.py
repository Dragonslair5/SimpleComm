import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *

class MQ_Allreduce_entry:
    def __init__ (self, rank, size, baseCycle):
        self.rank = rank;
        self.size = size;
        self.baseCycle = baseCycle;

class MQ_Allreduce:
    def __init__ (self, num_ranks, size):
        self.num_ranks = num_ranks;
        self.entries = [];
        self.size = size;
        #self.baseCycle = 0;
        self.baseCycle = [0] * num_ranks;
        self.op_name = "allreduce";
    
    def incEntry(self, allreduce_entry: MQ_Allreduce_entry):
        assert isinstance(allreduce_entry, MQ_Allreduce_entry);
        self.entries.append(allreduce_entry);
        #if self.baseCycle < allreduce_entry.baseCycle:
            #self.baseCycle = allreduce_entry.baseCycle;
        self.baseCycle[allreduce_entry.rank] = allreduce_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries);

    
    def process(self, algorithm: str) -> list:
        assert self.isReady();

        if (algorithm == "reduce_bcast"):
            return self.algorithm_reduce_bcast();

        print( bcolors.FAIL + "ERROR: Unknown Allreduce algorithm " + algorithm + bcolors.ENDC);
        sys.exit(1);

        
    # Based on SimGrid
    # allreduce__default (smpi_default_selector.cpp)
    # [1](reduce->0)   [2](0->bcast)
    def algorithm_reduce_bcast(self)-> list:
        sr_list = []

        # [1] Reduce
        for rank in range(1, self.num_ranks):
            # Current rank to rank 0 (reduce)
            sr = SendRecv(MPIC_SEND, rank, 0, self.size, self.baseCycle[rank], "allreduce", tag=MPIC_COLL_TAG_ALLREDUCE, col_id=1);
            sr_list.append(sr)
            sr = SendRecv(MPIC_RECV, 0, rank, self.size, self.baseCycle[0], "allreduce", tag=MPIC_COLL_TAG_ALLREDUCE, col_id=1);
            sr_list.append(sr)
            # NOTE Rank 0 to current rank (+1? on baseCycle to postpond the bcast from the reduce)
            #                             (Or the order is enough?)

        # [2] Binomial tree bcast
        root = 0;
        for rank in range(self.num_ranks):
            mask = 0x1;
            if rank >= root:
                relative_rank = rank - root;
            else:
                relative_rank = rank - root + self.num_ranks;
            while mask < self.num_ranks:
                if relative_rank & mask:
                    src = rank - mask;
                    if src < 0:
                        src = src + self.num_ranks;
                    sr = SendRecv(MPIC_RECV, rank, src, self.size, self.baseCycle[rank], operation_origin=self.op_name, tag=MPIC_COLL_TAG_ALLREDUCE, col_id=-1);
                    sr_list.append(sr);
                    break;
                mask = mask << 1;
            
            mask = mask >> 1;
            while mask > 0:
                if relative_rank + mask < self.num_ranks:
                    dst = rank + mask;
                    if dst >= self.num_ranks:
                        dst = dst - self.num_ranks;
                    sr = SendRecv(MPIC_SEND, rank, dst, self.size, self.baseCycle[rank], operation_origin=self.op_name, tag=MPIC_COLL_TAG_ALLREDUCE, col_id=-1);
                    sr_list.append(sr);
                mask = mask >> 1;


        # Adjust position for latency increment in bcast
        layer = []
        layer.append(0) # Adding root
        L = 2;
        while True:
            newLayer = []
            for i in range(0, len(sr_list)):
                send_recv = sr_list[i];
                if send_recv.kind == MPIC_SEND and (send_recv.rank in layer) and send_recv.col_id == -1:
                    send_recv.col_id = L;
                elif send_recv.kind == MPIC_RECV and (send_recv.partner in layer) and send_recv.col_id == -1:
                    send_recv.col_id = L;
                    newLayer.append(send_recv.rank)
            if len(newLayer) == 0:
                break;
            L = L + 1;
            layer = newLayer;
        
        return sr_list;