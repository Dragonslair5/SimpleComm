from tp_utils import *
from MPI_Constants import *
from SendRecv import *

class MQ_Alltoall_entry:
    def __init__ (self, rank, sendsize, recvsize, baseCycle):
        self.rank = rank;
        self.sendsize = sendsize;
        self.recvsize = recvsize;
        self.baseCycle = baseCycle;

class MQ_Alltoall:
    def __init__ (self, num_ranks, recvsize, sendsize):
        self.num_ranks = num_ranks;
        self.entries = [];
        self.sendsize = sendsize;
        self.recvsize = recvsize;
        self.baseCycle = 0;
        self.op_name = "alltoall"
    
    def incEntry(self, alltoall_entry: MQ_Alltoall_entry):
        assert isinstance(alltoall_entry, MQ_Alltoall_entry);
        self.entries.append(alltoall_entry);
        if self.baseCycle < alltoall_entry.baseCycle:
            self.baseCycle = alltoall_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries);

    # Based on SimGrid
    # alltoall__basic_linear (alltoall-basic-linear.cpp)
    def process(self):
        assert self.isReady();
        sr_list = []

        for i in range(self.num_ranks):
            rank = self.entries[i].rank;
            




        self.size = self.entries[0].sendsize;

        # [1] Reduce
        for rank in range(1, self.num_ranks):
            # Current rank to rank 0 (reduce)
            sr = SendRecv(MPIC_SEND, rank, 0, self.size, self.baseCycle, "allreduce");
            sr_list.append(sr)
            sr = SendRecv(MPIC_RECV, 0, rank, self.size, self.baseCycle, "allreduce");
            sr_list.append(sr)
            # Rank 0 to current rank (+1? on baseCycle to postpond the bcast from the reduce)
            #                        (Or the order is enough?)
        '''
        # [2] Naive bcast
        for rank in range(1, self.num_ranks):    
            sr = SendRecv(MPIC_SEND, 0, rank, self.size, self.baseCycle);
            sr_list.append(sr)
            sr = SendRecv(MPIC_RECV, rank, 0, self.size, self.baseCycle);
            sr_list.append(sr)
            

        print(sr_list)
        print("size of list -> " + str(len(sr_list)))
        return sr_list;
        '''

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
                    sr = SendRecv(MPIC_RECV, rank, src, self.size, self.baseCycle, "allreduce");
                    sr_list.append(sr);
                    break;
                mask = mask << 1;
            
            mask = mask >> 1;
            while mask > 0:
                if relative_rank + mask < self.num_ranks:
                    dst = rank + mask;
                    if dst >= self.num_ranks:
                        dst = dst - self.num_ranks;
                    sr = SendRecv(MPIC_SEND, rank, dst, self.size, self.baseCycle, "allreduce");
                    sr_list.append(sr);
                mask = mask >> 1;
        

        #print(sr_list)
        #print("size of list -> " + str(len(sr_list)))
        return sr_list;