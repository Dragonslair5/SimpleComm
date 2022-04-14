import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *

class MQ_Alltoallv_entry:
    def __init__ (self, rank: int, sendsize: int, recvsize: int, send_vector, recv_vector, baseCycle):
        self.rank = rank;
        self.sendsize = sendsize;
        self.send_vector = send_vector;
        self.recvsize = recvsize;
        self.recv_vector = recv_vector;
        self.baseCycle = baseCycle;

class MQ_Alltoallv:
    def __init__ (self, num_ranks):
        self.num_ranks = num_ranks;
        self.entries = [];
        #self.baseCycle = 0;
        #self.baseCycle = [0] * num_ranks;
        self.op_name = "alltoallv";
    
    def incEntry(self, alltoallv_entry: MQ_Alltoallv_entry):
        assert isinstance(alltoallv_entry, MQ_Alltoallv_entry);
        self.entries.append(alltoallv_entry);
        assert len(self.entries) <= self.num_ranks;
        #if self.baseCycle < alltoallv_entry.baseCycle:
        #    self.baseCycle = alltoallv_entry.baseCycle;
        #self.baseCycle[alltoallv_entry.rank] = alltoallv_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries);

    
    def process(self, algorithm: str) -> list:
        assert self.isReady();

        if (algorithm == "nbc_like_simgrid"):
            return self.algorithm_nbc_like_simgrid();
        if (algorithm == "nbc_improved"):
            return self.algorithm_nbc_improved();

        print( bcolors.FAIL + "ERROR: Unknown Alltoallv algorithm " + algorithm + bcolors.ENDC);
        sys.exit(1);
        

    # Based on SimGrid
    # ialltoallv (smpi_nbc_impl.cpp)
    def algorithm_nbc_like_simgrid(self)->list:
        sr_list = []

        # Post all receives first
        for ri in range(self.num_ranks):
            rank = self.entries[ri].rank;
            recvsize_datatype = self.entries[ri].recvsize;
            recv_vector = self.entries[ri].recv_vector;
            baseCycle = self.entries[ri].baseCycle;
            #print(str(rank) + " recv_vector = " + str(recv_vector));
            for i in range(self.num_ranks):
                #if i != rank and recv_vector[i] > 0:
                if i != rank:
                    recvsize = recvsize_datatype * recv_vector[i];
                    sr = SendRecv(MPIC_RECV, rank, i, recvsize, baseCycle, MPI_Operations.MPI_ALLTOALLV, operation_origin=self.op_name, tag=MPIC_COLL_TAG_ALLTOALLV, col_id=1);
                    sr_list.append(sr);
                    #print("alltoallv " + str(rank) + " <-- " + str(i));

        # Post all sends
        for ri in range(self.num_ranks):
            rank = self.entries[ri].rank;
            sendsize_datatype = self.entries[ri].sendsize;
            send_vector = self.entries[ri].send_vector;
            baseCycle = self.entries[ri].baseCycle;
            #print(str(rank) + " send_vector = " + str(send_vector));
            for i in range(self.num_ranks):
                #if i != rank and send_vector[i] > 0:
                if i != rank:
                    sendsize = sendsize_datatype * send_vector[i];
                    sr = SendRecv(MPIC_SEND, rank, i, sendsize, baseCycle, MPI_Operations.MPI_ALLTOALLV, operation_origin=self.op_name, tag=MPIC_COLL_TAG_ALLTOALLV, col_id=1);
                    sr_list.append(sr);
                    #print("alltoallv " + str(rank) + " --> " + str(i));

        return sr_list;

    # Improved version of "algorithm_nbc_like_simgrid", where null messages (size 0) are not sent
    def algorithm_nbc_improved(self)->list:
        sr_list = []

        # Post all receives first
        for ri in range(self.num_ranks):
            rank = self.entries[ri].rank;
            recvsize_datatype = self.entries[ri].recvsize;
            recv_vector = self.entries[ri].recv_vector;
            baseCycle = self.entries[ri].baseCycle;
            #print(str(rank) + " recv_vector = " + str(recv_vector));
            for i in range(self.num_ranks):
                if i != rank and recv_vector[i] > 0:
                    recvsize = recvsize_datatype * recv_vector[i];
                    sr = SendRecv(MPIC_RECV, rank, i, recvsize, baseCycle[rank], MPI_Operations.MPI_ALLTOALLV, operation_origin=self.op_name, tag=MPIC_COLL_TAG_ALLTOALLV);
                    sr_list.append(sr);
                    #print("alltoallv " + str(rank) + " <-- " + str(i));

        # Post all sends
        for ri in range(self.num_ranks):
            rank = self.entries[ri].rank;
            sendsize_datatype = self.entries[ri].sendsize;
            send_vector = self.entries[ri].send_vector;
            baseCycle = self.entries[ri].baseCycle;
            #print(str(rank) + " send_vector = " + str(send_vector));
            for i in range(self.num_ranks):
                if i != rank and send_vector[i] > 0:
                    sendsize = sendsize_datatype * send_vector[i];
                    sr = SendRecv(MPIC_SEND, rank, i, sendsize, baseCycle[rank], MPI_Operations.MPI_ALLTOALLV, operation_origin=self.op_name, tag=MPIC_COLL_TAG_ALLTOALLV);
                    sr_list.append(sr);
                    #print("alltoallv " + str(rank) + " --> " + str(i));

        return sr_list;