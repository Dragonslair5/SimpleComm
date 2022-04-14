import sys
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
        #self.baseCycle = 0;
        self.baseCycle = [0] * num_ranks;
        self.op_name = "bcast";
        
    def incEntry(self, bcast_entry: MQ_Bcast_entry):
        assert isinstance(bcast_entry, MQ_Bcast_entry);
        self.entries.append(bcast_entry)
        #print("bcast_entry at: " + str(bcast_entry.baseCycle))
        # self.ranks.append(rank);
        #if self.baseCycle < bcast_entry.baseCycle:
        #   self.baseCycle = bcast_entry.baseCycle;
        self.baseCycle[bcast_entry.rank] = bcast_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries)
        #return self.ready == self.num_ranks;

    # Based on SimGrid
    # bcast__binomial_tree (bcast-binomial-tree.cpp)
    def process(self, algorithm: str) -> list:
        assert self.num_ranks == len(self.entries)

        if (algorithm == "binomial_tree"):
            return self.algorithm_binomial_tree()

        print( bcolors.FAIL + "ERROR: Unknown Bcast algorithm " + algorithm + bcolors.ENDC);
        sys.exit(1);
        

    def algorithm_binomial_tree(self)->list:
        sr_list: list[SendRecv]
        sr_list = [];

        #for rank in range(0, self.num_ranks):
        for rank in range(self.num_ranks-1, -1, -1):
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
                    # (simgrid) recv
                    sr = SendRecv(MPIC_RECV, rank, src, self.size, self.baseCycle[rank], MPI_Operations.MPI_BCAST, "bcast", tag=MPIC_COLL_TAG_BCAST, col_id=-1);
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
                    # (simgrid) send
                    sr = SendRecv(MPIC_SEND, rank, dst, self.size, self.baseCycle[rank], MPI_Operations.MPI_BCAST, "bcast", tag=MPIC_COLL_TAG_BCAST, col_id=-1);
                    #print("R" + str(rank) + " -> " + "R" + str(dst));
                    sr_list.append(sr);
                    #print("Rank " + str(rank) + " sends to " + str(dst));
                mask = mask >> 1;
                #print(mask)

        #for i in range(len(sr_list)):
        #    print(sr_list[i])



        # Adjust position for latency increment (implemented with MPI_send)
        layer = []
        layer.append(self.root)
        startingPOS = [1] * self.num_ranks;
        
        while True:
            newLayer = []
            for i in range(0, len(sr_list)):
                send_recv = sr_list[i];
                if send_recv.kind == MPIC_SEND and (send_recv.rank in layer) and send_recv.col_id == -1:
                    send_recv.col_id = startingPOS[send_recv.rank];
                    startingPOS[send_recv.rank] = startingPOS[send_recv.rank] + 1;
                    startingPOS[send_recv.partner] = startingPOS[send_recv.rank];
                
            for i in range(0, len(sr_list)):
                send_recv = sr_list[i];
                if send_recv.kind == MPIC_RECV and (send_recv.partner in layer) and send_recv.col_id == -1:
                    send_recv.col_id = startingPOS[send_recv.rank] - 1;
                    newLayer.append(send_recv.rank);

            if len(newLayer) == 0:
                break;
            layer = newLayer;
        


        # Adjust position for latency increment (Implemented with MPI_isend)
        '''
        layer = []
        layer.append(self.root)
        L = 1;
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
        '''


        return sr_list;