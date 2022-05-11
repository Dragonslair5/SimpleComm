import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *
from CollectiveUtils import *


class Col_Alltoallv:

    # Based on SimGrid
    # ialltoallv (smpi_nbc_impl.cpp)
    @staticmethod
    def nbc_like_simgrid(num_ranks: int,
                     my_rank:int,
                     sendsize:int,
                     recvsize:int,
                     send_vector,
                     recv_vector,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        operation_origin="alltoallv";
        sr_list: typing.List[SendRecv];
        sr_list = [];

        
        # Post all receives first
        recvsize_datatype = recvsize;
        for i in range(num_ranks):
            if i != my_rank:
                recvsize = recvsize_datatype * recv_vector[i];
                sr = SendRecv(MPIC_RECV, my_rank, i, recvsize, baseCycle, MPI_Operations.MPI_ALLTOALLV, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLTOALLV, col_id=1);
                sr_list.append(sr);

        # Post all sends
        sendsize_datatype = sendsize;
        for i in range(num_ranks):
            if i != my_rank:
                sendsize = sendsize_datatype * send_vector[i];
                sr = SendRecv(MPIC_SEND, my_rank, i, sendsize, baseCycle, MPI_Operations.MPI_ALLTOALLV, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLTOALLV, col_id=1);
                sr_list.append(sr);

        return CollectiveUtils.layer_my_SendRecvList(sr_list=sr_list);


    # Improved version of "nbc_like_simgrid", where null messages (size 0) are not sent
    @staticmethod
    def improved_nbc_like_simgrid(num_ranks: int,
                     my_rank:int,
                     sendsize:int,
                     recvsize:int,
                     send_vector,
                     recv_vector,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        operation_origin="alltoallv";
        sr_list: typing.List[SendRecv];
        sr_list = [];

        # Post all receives first
        recvsize_datatype = recvsize;
        for i in range(num_ranks):
            if i != my_rank and recv_vector[i] > 0:
                recvsize = recvsize_datatype * recv_vector[i];
                sr = SendRecv(MPIC_RECV, my_rank, i, recvsize, baseCycle, MPI_Operations.MPI_ALLTOALLV, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLTOALLV, col_id=1);
                sr_list.append(sr);

        # Post all sends
        sendsize_datatype = sendsize;
        for i in range(num_ranks):
            if i != my_rank and send_vector[i] > 0:
                sendsize = sendsize_datatype * send_vector[i];
                sr = SendRecv(MPIC_SEND, my_rank, i, sendsize, baseCycle, MPI_Operations.MPI_ALLTOALLV, operation_origin=operation_origin, tag=MPIC_COLL_TAG_ALLTOALLV, col_id=1);
                sr_list.append(sr);
        

        return CollectiveUtils.layer_my_SendRecvList(sr_list=sr_list);