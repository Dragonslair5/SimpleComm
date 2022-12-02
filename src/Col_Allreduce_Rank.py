import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *
from CollectiveUtils import *


class Col_AllReduce:









    # Based on SimGrid
    # allreduce__default (smpi_default_selector.cpp)
    # [1](reduce->0)   [2](0->bcast)
    @staticmethod
    def reduce_bcast(num_ranks: int,
                     my_rank:int,
                     size:int,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        
        operation_origin="allreduce";
        sr_list: typing.List[SendRecv];
        sr_list = [];

        # [1] Reduce
        # Current rank to rank 0 (reduce)
        if my_rank == 0:
            for source_rank in range(1, num_ranks):
                sr = sr = SendRecv(MPIC_RECV, 0, source_rank, size, baseCycle, MPI_Operations.MPI_ALLREDUCE, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLREDUCE, col_id=1);
                sr_list.append(sr)
        else:
            sr = SendRecv(MPIC_SEND, my_rank, 0, size, baseCycle, MPI_Operations.MPI_ALLREDUCE, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLREDUCE, col_id=1);
            sr_list.append(sr)


        # [2] Binomial tree bcast
        root = 0;
        col_id = 2;

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

                sr = SendRecv(MPIC_RECV, my_rank, src, size, baseCycle, MPI_Operations.MPI_BCAST, operation_origin=operation_origin, tag=MPIC_COLL_TAG_BCAST, col_id=col_id);
                col_id = col_id + 1;
                sr_list.append(sr);
                break;
            mask = mask << 1;

        mask = mask >> 1;

        while mask > 0:
            if relative_rank + mask < num_ranks:
                dst = my_rank + mask;
                if dst >= num_ranks:
                    dst = dst - num_ranks;
                sr = SendRecv(MPIC_SEND, my_rank, dst, size, baseCycle*1.01, MPI_Operations.MPI_BCAST, operation_origin=operation_origin, tag=MPIC_COLL_TAG_BCAST, col_id=col_id);
                col_id = col_id + 1;
                sr_list.append(sr);
            mask = mask >> 1;

        
        #print("My Rank: " + str(my_rank) + " SR: " + str(len(sr_list)))

        return CollectiveUtils.layer_my_SendRecvList(sr_list=sr_list);





    # Based on SimGrid
    # allreduce__default (smpi_default_selector.cpp)
    # [1](reduce->0)   [2](0->bcast)
    @staticmethod
    def reduceBinomialTree_bcast(num_ranks: int,
                     my_rank:int,
                     size:int,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        
        operation_origin="allreduce";
        sr_list: typing.List[SendRecv];
        sr_list = [];
        col_id = 1;


        # [1] Reduce
        # [1] Current rank to rank 0
        mask: int;
        relrank: int;
        source: int;
        dst: int;

        root = 0;
        mask = 1;
        lroot = root;
        relrank = (my_rank - lroot + num_ranks) % num_ranks;

        while (mask < num_ranks):
            
            # RECV
            if ((mask & relrank) == 0):
                source = (relrank | mask);
                if (source < num_ranks):
                    source = (source + lroot) % num_ranks;
                    # recv
                    sr = SendRecv(MPIC_RECV, my_rank, source, size, baseCycle, MPI_Operations.MPI_REDUCE, operation_origin=operation_origin, tag=MPIC_COLL_TAG_REDUCE, col_id=col_id);
                    col_id = col_id + 1;
                    sr_list.append(sr);
                    #print(str(my_rank) + " <- " + str(source))
            # SEND
            else:
                dst = ((relrank & (~mask)) + lroot) % num_ranks;
                # send
                sr = SendRecv(MPIC_SEND, my_rank, dst, size, baseCycle, MPI_Operations.MPI_REDUCE, operation_origin=operation_origin, tag=MPIC_COLL_TAG_REDUCE, col_id=col_id);
                col_id = col_id + 1;
                sr_list.append(sr);
                #print(str(my_rank) + " -> " + str(dst))
                break;            
            mask = mask << 1;


        # [2] Binomial tree bcast
        root = 0;
        #col_id = 2;

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

                sr = SendRecv(MPIC_RECV, my_rank, src, size, baseCycle, MPI_Operations.MPI_BCAST, operation_origin=operation_origin, tag=MPIC_COLL_TAG_BCAST, col_id=col_id);
                col_id = col_id + 1;
                sr_list.append(sr);
                break;
            mask = mask << 1;

        mask = mask >> 1;

        while mask > 0:
            if relative_rank + mask < num_ranks:
                dst = my_rank + mask;
                if dst >= num_ranks:
                    dst = dst - num_ranks;
                sr = SendRecv(MPIC_SEND, my_rank, dst, size, baseCycle*1.01, MPI_Operations.MPI_BCAST, operation_origin=operation_origin, tag=MPIC_COLL_TAG_BCAST, col_id=col_id);
                col_id = col_id + 1;
                sr_list.append(sr);
            mask = mask >> 1;

        
        #print("My Rank: " + str(my_rank) + " SR: " + str(len(sr_list)))

        return CollectiveUtils.layer_my_SendRecvList(sr_list=sr_list);
