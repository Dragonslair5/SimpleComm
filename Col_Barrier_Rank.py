import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *


class Col_Barrier:



    @staticmethod
    def basic_linear(num_ranks: int,
                     my_rank:int,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        
        operation_origin="barrier"
        sr_list = []

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


        return sr_list;
