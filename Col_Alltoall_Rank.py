import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *
from CollectiveUtils import *


class Col_Alltoall:


    @staticmethod
    def basic_linear_or_ring(num_ranks: int,
                     my_rank:int,
                     sendsize:int,
                     recvsize:int,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        if sendsize < 3000:
            return Col_Alltoall.basic_linear(num_ranks, my_rank, sendsize, recvsize, baseCycle, rank_offset);
        else:
            return Col_Alltoall.ring(num_ranks, my_rank, sendsize, recvsize, baseCycle, rank_offset);



    # Based on SimGrid
    # alltoall__basic_linear (alltoall-basic-linear.cpp)
    @staticmethod
    def basic_linear(num_ranks: int,
                     my_rank:int,
                     sendsize:int,
                     recvsize:int,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        
        operation_origin="alltoall";
        sr_list: typing.List[SendRecv];
        sr_list = [];

        # Post all receives first
        i = (my_rank + 1) % num_ranks;
        while i != my_rank:
            sr = SendRecv(MPIC_RECV, my_rank, i, recvsize , baseCycle, MPI_Operations.MPI_ALLTOALL, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLTOALL, col_id=1);
            #print("alltoall " + str(rank) + " --> " + str(i));
            sr_list.append(sr);
            i = (i+1) % num_ranks;

        # Post all sends in reverse order
        i = (my_rank + num_ranks - 1) % num_ranks;
        while i != my_rank:
            sr = SendRecv(MPIC_SEND, my_rank, i, sendsize, baseCycle, MPI_Operations.MPI_ALLTOALL, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLTOALL, col_id=1);
            #print("alltoall " + str(rank) + " <-- " + str(i));
            sr_list.append(sr);
            i = (i + num_ranks - 1) % num_ranks;
        

        return CollectiveUtils.layer_my_SendRecvList(sr_list=sr_list);


    # Based on SimGrid
    # alltoall__ring (alltoall-ring.cpp)
    @staticmethod
    def ring(num_ranks: int,
                     my_rank:int,
                     sendsize:int,
                     recvsize:int,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        
        operation_origin="alltoall";
        sr_list: typing.List[SendRecv];
        sr_list = [];

        

        # Post all receives first
        col_id = 1;
        i = (my_rank + 1) % num_ranks;
        while i != my_rank:
            sr = SendRecv(MPIC_RECV, my_rank, i, recvsize , baseCycle, MPI_Operations.MPI_ALLTOALL, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLTOALL, col_id=col_id);
            #print("alltoall " + str(rank) + " --> " + str(i));
            sr_list.append(sr);
            i = (i+1) % num_ranks;
            col_id = col_id + 1

        # Post all sends in reverse order
        col_id = 1;
        i = (my_rank + num_ranks - 1) % num_ranks;
        while i != my_rank:
            sr = SendRecv(MPIC_SEND, my_rank, i, sendsize, baseCycle, MPI_Operations.MPI_ALLTOALL, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLTOALL, col_id=col_id);
            #print("alltoall " + str(rank) + " <-- " + str(i));
            sr_list.append(sr);
            i = (i + num_ranks - 1) % num_ranks;
            col_id = col_id + 1

        return CollectiveUtils.layer_my_SendRecvList(sr_list=sr_list)

