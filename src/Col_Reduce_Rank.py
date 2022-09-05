import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *
from CollectiveUtils import *


class Col_Reduce:


    # Based on SimGrid
    # colls::ireduce (smpi_nbc_impl.cpp)
    @staticmethod
    def allToRoot(num_ranks: int,
                     my_rank:int,
                     size:int,
                     root:int,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        
        operation_origin="reduce";
        sr_list: typing.List[SendRecv];
        sr_list = [];

        if my_rank != root:
            sr = SendRecv(MPIC_SEND, my_rank, root, size, baseCycle, MPI_Operations.MPI_REDUCE, operation_origin=operation_origin, tag=MPIC_COLL_TAG_REDUCE, col_id=1);
            sr_list.append(sr);
        else:
            for i in range(num_ranks):
                if i != my_rank:
                    sr = SendRecv(MPIC_RECV, my_rank, i, size, baseCycle, MPI_Operations.MPI_REDUCE, operation_origin=operation_origin, tag=MPIC_COLL_TAG_REDUCE, col_id=1);
                    sr_list.append(sr);

        return CollectiveUtils.layer_my_SendRecvList(sr_list=sr_list);
