import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *




class Col_Bcast:

    #def __init__ (self, num_ranks: int):
    #    self.num_ranks = num_ranks;

    @staticmethod
    def binomial_tree(num_ranks: int, 
                            my_rank: int, 
                            root: int,
                            size: int,
                            baseCycle: float, 
                            rank_offset = 0)->list:
                            
        sr_list: list[SendRecv]
        sr_list = [];

        mask = 0x1;

        if my_rank >= root:
            relative_rank = my_rank - root;
        else:
            relative_rank = my_rank - root + num_ranks;


        while mask < num_ranks:
            if relative_rank & mask:
                src = my_rank - mask;
                if src < 0:
                    src = src + num_ranks;

                sr = SendRecv(MPIC_RECV, my_rank, src, size, baseCycle, MPI_Operations.MPI_BCAST, "bcast", tag=MPIC_COLL_TAG_BCAST, col_id=-1);
                sr_list.append(sr);
                break;
            mask = mask << 1;

        mask = mask >> 1;

        while mask > 0:
            if relative_rank + mask < num_ranks:
                dst = my_rank + mask;
                if dst >= num_ranks:
                    dst = dst - num_ranks;
                sr = SendRecv(MPIC_SEND, my_rank, dst, size, baseCycle*1.01, MPI_Operations.MPI_BCAST, "bcast", tag=MPIC_COLL_TAG_BCAST, col_id=-1);
                sr_list.append(sr);
            mask = mask >> 1;

        return sr_list;