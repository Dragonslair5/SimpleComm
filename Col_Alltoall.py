import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *

class MQ_Alltoall_entry:
    def __init__ (self, rank, sendsize, recvsize, baseCycle):
        self.rank = rank;
        self.sendsize = sendsize;
        self.recvsize = recvsize;
        self.baseCycle = baseCycle;

class MQ_Alltoall:
    def __init__ (self, num_ranks, recvsize, sendsize):
        self.num_ranks = num_ranks;
        self.entries = [];
        self.sendsize = sendsize;
        self.recvsize = recvsize;
        self.baseCycle = 0;
        self.op_name = "alltoall";
    
    def incEntry(self, alltoall_entry: MQ_Alltoall_entry):
        assert isinstance(alltoall_entry, MQ_Alltoall_entry);
        self.entries.append(alltoall_entry);
        if self.baseCycle < alltoall_entry.baseCycle:
            self.baseCycle = alltoall_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries);

    
    def process(self, algorithm: str) -> list:
        assert self.isReady();

        if (algorithm == "basic_linear"):
            return self.algorithm_basic_linear();

        print( bcolors.FAIL + "ERROR: Unknown Alltoall algorithm " + algorithm + bcolors.ENDC);
        sys.exit(1);
        

    # Based on SimGrid
    # alltoall__basic_linear (alltoall-basic-linear.cpp)
    def algorithm_basic_linear(self)->list:
        sr_list = []

        # Post all receives first
        for ri in range(self.num_ranks):
            rank = self.entries[ri].rank;
            recvsize = self.entries[ri].recvsize;
            baseCycle = self.entries[ri].baseCycle;
            i = (rank + 1) % self.num_ranks;
            while i != rank:
                sr = SendRecv(MPIC_RECV, rank, i, recvsize , baseCycle, operation_origin=self.op_name, tag=MPIC_COLL_TAG_ALLTOALL, col_id=0);
                #print("alltoall " + str(rank) + " --> " + str(i));
                sr_list.append(sr);
                i = (i+1) % self.num_ranks;
           
        # Post all sends in reverse order
        for ri in range(self.num_ranks):
            rank = self.entries[ri].rank;
            sendsize = self.entries[ri].sendsize;
            baseCycle = self.entries[ri].baseCycle;
            i = (rank + self.num_ranks - 1) % self.num_ranks;
            while i != rank:
                sr = SendRecv(MPIC_SEND, rank, i, sendsize, baseCycle, operation_origin=self.op_name, tag=MPIC_COLL_TAG_ALLTOALL, col_id=0);
                #print("alltoall " + str(rank) + " <-- " + str(i));
                sr_list.append(sr);
                i = (i + self.num_ranks - 1) % self.num_ranks;

        return sr_list;