from tp_utils import *
from MPI_Constants import *
from SendRecv import *


class MQ_Bcast_entry:
    def __init__ (self, rank, root, size, baseCycle):
        self.rank = rank;
        self.root = root;
        self.size = size;
        self.baseCycle = baseCycle;


class MQ_Bcast:
    def __init__(self, num_ranks, root, size):
        self.num_ranks = num_ranks;
        self.entries = [];
        self.root = root;
        self.size = size;
        self.baseCycle = 0;
        self.op_name = "bcast";
        
    def incEntry(self, bcast_entry: MQ_Bcast_entry):
        assert isinstance(bcast_entry, MQ_Bcast_entry);
        self.entries.append(bcast_entry)
        #print("bcast_entry at: " + str(bcast_entry.baseCycle))
        # self.ranks.append(rank);
        if self.baseCycle < bcast_entry.baseCycle:
            self.baseCycle = bcast_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries)
        #return self.ready == self.num_ranks;

    # Based on SimGrid
    # bcast__binomial_tree (bcast-binomial-tree.cpp)
    def process(self):
        assert self.num_ranks == len(self.entries)
        sr_list = [];

        for rank in range(self.num_ranks):
            mask = 0x1;
            #print(mask)
            #relative_rank = (rank >= self.root) if rank - self.root else rank - self.root + self.num_ranks;
            if rank >= self.root:
                relative_rank = rank - self.root;
            else:
                relative_rank = rank - self.root + self.num_ranks;

            #print("Rank "+str(rank) + " relative rank "+str(relative_rank) + " root " + str(self.root))
            while mask < self.num_ranks:
                if relative_rank & mask:
                    src = rank - mask;
                    if src < 0:
                        src = src + self.num_ranks;
                    #print("Rank " + str(rank) + " received from " + str(src));
                    sr = SendRecv(MPIC_RECV, rank, src, self.size, self.baseCycle, "bcast");
                    sr_list.append(sr);
                    break;
                mask = mask << 1;
                #print(mask)

            mask = mask >> 1;
            #print(mask)
            while mask > 0:
                if relative_rank + mask < self.num_ranks:
                    dst = rank + mask;
                    if dst >= self.num_ranks:
                        dst = dst - self.num_ranks;
                    sr = SendRecv(MPIC_SEND, rank, dst, self.size, self.baseCycle, "bcast");
                    sr_list.append(sr);
                    #print("Rank " + str(rank) + " sends to " + str(dst));
                mask = mask >> 1;
                #print(mask)

        #for i in range(len(sr_list)):
        #    print(sr_list[i])

        return sr_list;