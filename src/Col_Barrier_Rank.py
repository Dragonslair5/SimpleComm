import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *
from CollectiveUtils import *


class Col_Barrier:



    @staticmethod
    def basic_linear(num_ranks: int,
                     my_rank:int,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        
        operation_origin="barrier";
        sr_list: typing.List[SendRecv];
        sr_list = [];

        if my_rank == 0:
            for rank in range(1, num_ranks):
                # (simgrid) irecv
                sr = SendRecv(MPIC_RECV, my_rank, rank, 0, baseCycle, MPI_Operations.MPI_BARRIER, operation_origin=operation_origin, tag=MPIC_COLL_TAG_BARRIER, blocking=True, col_id=1);
                sr_list.append(sr);

            for rank in range(1, num_ranks):
                # Rank 0 to current rank
                # (simgrid) isend
                sr = SendRecv(MPIC_SEND, my_rank, rank, 0, baseCycle, MPI_Operations.MPI_BARRIER, operation_origin=operation_origin, tag=MPIC_COLL_TAG_BARRIER, blocking=True, col_id=2);
                sr_list.append(sr);
        else:
            sr = SendRecv(MPIC_SEND, my_rank, 0, 0, baseCycle, MPI_Operations.MPI_BARRIER, operation_origin=operation_origin, tag=MPIC_COLL_TAG_BARRIER, blocking=True, col_id=1);
            sr_list.append(sr);
            sr = SendRecv(MPIC_RECV, my_rank, 0, 0, baseCycle, MPI_Operations.MPI_BARRIER, operation_origin=operation_origin, tag=MPIC_COLL_TAG_BARRIER, blocking=True, col_id=2);
            sr_list.append(sr);


        return CollectiveUtils.layer_my_SendRecvList(sr_list=sr_list);


        layered_list = [];
        layered_list.append([]);

        for i in range(len(sr_list)):
            col_id = sr_list[i].col_id;
            if len(layered_list) < col_id:
                layered_list.append([])
            layered_list[col_id-1].append(sr_list[i]);


        return layered_list;
