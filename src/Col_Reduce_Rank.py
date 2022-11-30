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


    
    # Based on SimGrid
    # simgrid::smpi:reduce__binomial (src/smpi/colls/reduce/reduce-binomial.cpp)
    @staticmethod
    def binomial_tree(num_ranks: int,
                     my_rank:int,
                     size:int,
                     root:int,
                     baseCycle: float, 
                     rank_offset = 0)->list:

        operation_origin="reduce";
        sr_list: typing.List[SendRecv];
        sr_list = [];

        mask: int;
        relrank: int;
        source: int;
        dst: int;

        mask = 1;
        lroot = root;
        relrank = (my_rank - lroot + num_ranks) % num_ranks;

        col_id = 1;

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
        
        # -----
        # I am unsure about this last part
        # TODO: this should be checked, if we need this or not.
        # The original condition was the following
        #   -->  if (not is_commutative && (root != 0))
        # We do not check the reduce operation, so we assume it is always commutative
        # -----
        #if root != 0: #
        #    if my_rank == 0:
        #        sr = SendRecv(MPIC_SEND, my_rank, root, size, baseCycle, MPI_Operations.MPI_REDUCE, operation_origin=operation_origin, tag=MPIC_COLL_TAG_REDUCE, col_id=2);
        #        sr_list.append(sr);
        #    elif my_rank == root:
        #        sr = SendRecv(MPIC_RECV, my_rank, 0, size, baseCycle, MPI_Operations.MPI_REDUCE, operation_origin=operation_origin, tag=MPIC_COLL_TAG_REDUCE, col_id=1);
        #        sr_list.append(sr);

        return CollectiveUtils.layer_my_SendRecvList(sr_list=sr_list);