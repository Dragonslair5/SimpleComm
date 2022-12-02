
import sys

from tp_utils import *
from MPI_Constants import *
from SendRecv import *

from Col_Bcast_Rank import *
from Col_Barrier_Rank import *
from Col_Allreduce_Rank import *
from Col_Alltoall_Rank import *
from Col_Alltoallv_Rank import *
from Col_Reduce_Rank import *





class iSendRecv:

    def __init__(self, tag, endCycle, operation_ID):
        self.tag = tag;
        self.endCycle = endCycle;
        self.operation_ID = operation_ID;



class Rank:

    S_NORMAL=0 # Not Comunicating
    S_COMMUNICATING=2 # Is Communicating
    S_ENDED=1 # Trace has ended
    S_SR=3 # sent a SEND or RECV (unused)
    S_M=4 # has a Match, waiting for result (unused)
    S_WAITING=5; # Using WAIT or WAITALL for unblocking operations

    def __init__(self, nRanks:int , rank: int, trace: list, configfile: SimpleCommConfiguration):
        self.nRanks = nRanks;
        self.rank = rank;
        self.trace = trace;
        self.trace_size = len(trace)
        self.index = 0;
        self.state = Rank.S_NORMAL; # start at normal
        self.cycle = 0; # CurrentCycle
        self.current_operation = "";
        self.simulateComputation = configfile.computation;
        self.processing_speed = configfile.processing_speed;
        self.i_am_blocked_by_standard_send_or_recv = False;
        
        # Non-blocking operation
        self.iSendRecvQ = [];
        self.waitall = 0;
        self.waitingTag = None;
        self.shallEnd = False; # To allow a last operation to be done before finalizing (Barrier)

        # Statistics data
        self.timeHaltedDueCommunication = 0;
        self.amountOfDataOnCommunication = 0;
        self.amountOfCommunications = 0;
        self.largestDataOnASingleCommunication = 0;
        

        #self.dict_mpi_overhead = {key:value for key, value in MPI_Operations.__dict__.items() if not key.startswith('__') and not callable(key)}
        self.dict_mpi_overhead = {key:value for key, value in MPI_Operations.__dict__.items() if key.startswith('MPI_')}
        self.dict_mpi_overhead = dict.fromkeys(self.dict_mpi_overhead, 0);

        # Modifiers
        # NOTE: boosterFactor is currently implemented on the creation of the match (MessageQueue/CheckMatch)


        # ***********
        #   ____      _ _           _   _                
        #  / ___|___ | | | ___  ___| |_(_)_   _____  ___ 
        # | |   / _ \| | |/ _ \/ __| __| \ \ / / _ \/ __|
        # | |__| (_) | | |  __/ (__| |_| |\ V /  __/\__ \
        #  \____\___/|_|_|\___|\___|\__|_| \_/ \___||___/
        #                                                
        # ***********

        self.counter_waitingCollective = 0;
        self.collective_sr_list = None;

        # **********************   Collective Algorithms   **********************
        # Barrier
        self.col_barrier = None;
        if configfile.CA_Barrier == "basic_linear":
            self.col_barrier = Col_Barrier.basic_linear;
        else:
            self.printErrorAndQuit("ERROR: Unknown Barrier algorithm " + configfile.CA_Barrier);
        # Bcast
        self.col_bcast = None;
        if configfile.CA_Bcast == "binomial_tree":
            self.col_bcast = Col_Bcast.binomial_tree;
        else:
            self.printErrorAndQuit("ERROR: Unknown Bcast algorithm " + configfile.CA_Bcast);
        # Reduce
        self.col_reduce = None;
        if configfile.CA_Reduce == "alltoroot":
            self.col_reduce = Col_Reduce.allToRoot;
        elif configfile.CA_Reduce == "binomial_tree":
            self.col_reduce = Col_Reduce.binomial_tree;
        else:
            self.printErrorAndQuit("ERROR: Unknown Reduce algorithm " + configfile.CA_Reduce);
        # Allreduce
        self.col_allreduce = None;
        if configfile.CA_Allreduce == "reduce_bcast":
            self.col_allreduce = Col_AllReduce.reduce_bcast;
        elif configfile.CA_Allreduce == "reduceBinomialTree_bcast":
            self.col_allreduce = Col_AllReduce.reduceBinomialTree_bcast
        else:
            self.printErrorAndQuit("ERROR: Unknown Allreduce algorithm " + configfile.CA_Allreduce);
        # Alltoall
        self.col_alltoall = None;
        if configfile.CA_Alltoall == "basic_linear":
            self.col_alltoall = Col_Alltoall.basic_linear_or_ring;
        else:
            self.printErrorAndQuit("ERROR: Unknown Alltoall algorithm " + configfile.CA_Alltoall);
        # Alltoallv
        self.col_alltoallv = None;
        if configfile.CA_Alltoallv == "nbc_like_simgrid":
            self.col_alltoallv = Col_Alltoallv.nbc_like_simgrid;
        elif configfile.CA_Alltoallv == "nbc_improved":
            self.col_alltoallv = Col_Alltoallv.improved_nbc_like_simgrid;
        else:
            self.printErrorAndQuit("ERROR: Unknown Alltoallv algorithm " + configfile.CA_Alltoallv);
        # ***********************************************************************

    def printErrorAndQuit(self, error_message: str):
        print( bcolors.FAIL + error_message + bcolors.ENDC);
        sys.exit(1);


    def includeHaltedTime(self, begin: float, end: float, operation_ID: int):
        #if end < begin:
        #    return None;
        assert end >= begin, "end must be greater than begin"
        time = end - begin
        self.timeHaltedDueCommunication = self.timeHaltedDueCommunication + time;

        operationName=MPI_Operations.getOperationNameByID(operation_ID);
        self.dict_mpi_overhead[operationName] = self.dict_mpi_overhead[operationName] + time


    def getCurrentStateName(self):
        if self.state == Rank.S_NORMAL:
            return "NORMAL";
        if self.state == Rank.S_COMMUNICATING:
            return "COMMUNICATING";
        if self.state == Rank.S_ENDED:
            return "ENDED";
        if self.state == Rank.S_WAITING:
            return "WAITING";
        return "Unknown";


    def include_iSendRecvConclusion(self, tag, endcycle, operation_ID):
        isendrecv = iSendRecv(tag, endcycle, operation_ID);
        self.iSendRecvQ.append(isendrecv);
        self.waitall = self.waitall + 1;

    def check_iSendRecvConclusion(self, tag):
        assert self.state == Rank.S_WAITING;
        #assert self.waitingTag != None;
        if self.waitingTag == None: # Waitall
            if self.waitall == 0:
                lenght = len(self.iSendRecvQ)
                #print("Rank " + str(self.rank) + " cycle - " + str(self.cycle))
                for i in range(lenght):
                    #print(str(i) + " - isendrecv end cycle: " + str(self.iSendRecvQ[0].endCycle))
                    if self.cycle < self.iSendRecvQ[0].endCycle:
                        self.includeHaltedTime(self.cycle, self.iSendRecvQ[0].endCycle, self.iSendRecvQ[0].operation_ID);
                        self.cycle = self.iSendRecvQ[0].endCycle;
                    del self.iSendRecvQ[0];
                return True;
        else: # Wait
            for i in range(len(self.iSendRecvQ)):
                if tag == self.iSendRecvQ[i].tag:
                    #print(bcolors.WARNING + " Rank "+ str(self.rank) + " found tag " + str(tag) + bcolors.ENDC);
                    if self.cycle < self.iSendRecvQ[i].endCycle:
                        self.includeHaltedTime(self.cycle, self.iSendRecvQ[i].endCycle, self.iSendRecvQ[i].operation_ID);
                        self.cycle = self.iSendRecvQ[i].endCycle;
                    del self.iSendRecvQ[i];
                    self.waitall = self.waitall - 1;
                    self.current_operation = self.current_operation + " (iM)";
                    self.waitingTag = None;
                    return True; # Found completed iSendRecv
        
        return False; # No completed operation found with current waiting tag
                      # Nor all isend/irecv arrived for waitall


    def concludeCollectiveSendRecv(self):
        #print("down Rank: " + str(self.rank) + " Counter: " + str(self.counter_waitingCollective))
        assert self.counter_waitingCollective > 0, "Wtf?"
        self.counter_waitingCollective = self.counter_waitingCollective - 1;
        #print("down Rank: " + str(self.rank) + " Counter: " + str(self.counter_waitingCollective))

    def canGoToNormal(self)->bool:
        if self.collective_sr_list is None:
            return True;
        return False;

    def step(self, num_ranks):
        #if self.state == Rank.S_ENDED or self.state == Rank.S_COMMUNICATING:
        #    return None;
        if self.state == Rank.S_ENDED:
            return None;
        if self.state == Rank.S_COMMUNICATING:
            if self.counter_waitingCollective != 0:
                return None;
            if (self.collective_sr_list != None):
                if (len(self.collective_sr_list) > 0):
                    self.counter_waitingCollective = len(self.collective_sr_list[0]);
                    #print("up Rank: " + str(self.rank) + " Counter: " + str(self.counter_waitingCollective))

                    # Grab next bunch of SendRecv from collective
                    sr_list: typing.List[SendRecv];
                    sr_list = self.collective_sr_list.pop(0);
                    # Update baseCycle
                    for i in range(len(sr_list)):
                        sr_list[i].baseCycle = self.cycle;
                    
                    #print("Rank " + str(self.rank) + " sending " + str(len(sr_list)) + " SR on " + str(self.cycle))

                    return sr_list;
                #print("Rank " + str(self.rank) +" Going to normal")
                self.collective_sr_list = None;
                self.state = Rank.S_NORMAL;
                if self.shallEnd:
                    self.state = Rank.S_ENDED;
                    return None;
            else:
                    return None; # This is called when this rank is blocked by an usual MPI_Send on MPI_Recv.

        if self.state == Rank.S_WAITING:
            if(self.check_iSendRecvConclusion(self.waitingTag)):
                self.state = Rank.S_NORMAL;
            return None;
        if self.shallEnd and (self.collective_sr_list is None):
            self.state = Rank.S_ENDED;
            return None;
        #if self.counter_waitingCollective != 0:
        #    print("Dae counter: " + str(self.counter_waitingCollective))
        #    return None;
        #if (self.collective_sr_list != None):
        #    if (len(self.collective_sr_list[0]) > 0):
        #        self.counter_waitingCollective = len(self.collective_sr_list[0]);
        #        print("up Rank: " + str(self.rank) + " Counter: " + str(self.counter_waitingCollective))
        #        return self.collective_sr_list.pop(0);
        #    self.collective_sr_list = None;
        #    self.state = Rank.S_NORMAL;
        #    if self.shallEnd:
        #        self.state = Rank.S_ENDED;
        #        return None;



        # ********************************************
        #  ____ _____ _____ ____  
        # / ___|_   _| ____|  _ \ 
        # \___ \ | | |  _| | |_) |
        #  ___) || | | |___|  __/ 
        # |____/ |_| |_____|_|    
        # ********************************************
        # Based on the trace syntax found on SimGrid Time-Independent Traces (src/smpi/internals/smpi_replay.cpp)


        # Grab workload and increment index
        workload = self.trace[self.index];
        operation = workload[1];
        self.index = self.index + 1;

        # Consume workload
        if operation == "init":
            self.current_operation = "init-" + str(self.index);
            return None;
        if operation == "compute":
            self.current_operation = "compute-" + str(self.index);
            if self.simulateComputation:
                workload = int(float(workload[2])); # need to float->int because of scientific notation
                processing_cycles = workload / self.processing_speed;
                self.cycle = self.cycle + processing_cycles;
            return None;
        if(operation == "send"):
            self.state = Rank.S_COMMUNICATING;
            target=int(workload[2]);
            tag = int(workload[3]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_SEND, self.rank, target, size, self.cycle, MPI_Operations.MPI_SEND, "send", tag=tag);
            self.current_operation = "send(" + str(target) + ")-" + str(self.index);
            self.i_am_blocked_by_standard_send_or_recv = True;
            return sr;
        if(operation == "recv"):
            self.state = Rank.S_COMMUNICATING;
            source=int(workload[2]);
            tag = int(workload[3]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_RECV, self.rank, source, size, self.cycle, MPI_Operations.MPI_RECV, "recv", tag=tag);
            self.current_operation = "recv(" + str(source) + ")-" + str(self.index);
            self.i_am_blocked_by_standard_send_or_recv = True;
            return sr;





        # Collectives
        if(operation == "sendRecv"): # Implemented as collective
            self.state = Rank.S_COMMUNICATING;
            # Send
            sendTarget = int(workload[3])
            sendTag = 0;
            sendDatatype = getDataTypeSize(int(workload[6]));
            sendSize = int(workload[2]) * sendDatatype;
            sr_send = SendRecv(MPIC_SEND, self.rank, sendTarget, sendSize, self.cycle, MPI_Operations.MPI_SENDRECV, "sendrecv", blocking = False, tag = sendTag);
            # Recv
            recvSource = int(workload[5]);
            recvTag = 0;
            recvDataType = getDataTypeSize(int(workload[7]));
            recvSize = int(workload[4]) * recvDataType;
            sr_recv = SendRecv(MPIC_RECV, self.rank, recvSource, recvSize, self.cycle, MPI_Operations.MPI_SENDRECV, "sendrecv", blocking = False, tag = recvTag);
            
            self.current_operation = "SendRecv-(" + str(sendTarget) + " - " + str(recvSource) + ")-" + str(self.index);
            self.counter_waitingCollective = 2;
            return [sr_send, sr_recv];

        if(operation == "bcast"):
            self.current_operation = "bcast-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            root = int(workload[3]);
            datatype = getDataTypeSize(int(workload[4]));
            size = int(workload[2]) * datatype;
            self.collective_sr_list = self.col_bcast(self.nRanks, self.rank, root, size, self.cycle, rank_offset=0);
            self.counter_waitingCollective = len(self.collective_sr_list[0]);
            return self.collective_sr_list.pop(0);

            

        if(operation == "barrier"):
            self.current_operation = "barrier-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            
            self.collective_sr_list = self.col_barrier(self.nRanks, self.rank, self.cycle, rank_offset=0);
            self.counter_waitingCollective = len(self.collective_sr_list[0]);
            return self.collective_sr_list.pop(0);


        if(operation == "reduce"):
            self.current_operation = "reduce-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            root = int(workload[4]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[2]) * datatype;
            self.collective_sr_list = self.col_reduce(self.nRanks, self.rank, size, root, self.cycle, rank_offset=0);
            self.counter_waitingCollective = len(self.collective_sr_list[0]);
            return self.collective_sr_list.pop(0);

        if(operation == "allreduce"):
            self.current_operation = "allreduce-"+str(self.index);
            self.state = Rank.S_COMMUNICATING;
            datatype = getDataTypeSize(int(workload[4]));
            size = int(workload[2]) * datatype;
            self.collective_sr_list = self.col_allreduce(self.nRanks, self.rank, size, self.cycle, rank_offset=0);
            self.counter_waitingCollective = len(self.collective_sr_list[0]);
            return self.collective_sr_list.pop(0);

        if(operation == "alltoall"):
            self.current_operation = "alltoall-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            send_datatype = getDataTypeSize(int(workload[4]));
            recv_datatype = getDataTypeSize(int(workload[5]));
            send_size = int(workload[2]) * send_datatype;
            recv_size = int(workload[3]) * recv_datatype;
            self.collective_sr_list = self.col_alltoall(self.nRanks, self.rank, send_size, recv_size, self.cycle, rank_offset=0);
            self.counter_waitingCollective = len(self.collective_sr_list[0]);
            return self.collective_sr_list.pop(0);

        if(operation == "alltoallv"): # alltoallv <send count> [send vector] <recv count> [recv vector] <recv datatype> <send datatype>
            self.current_operation = "alltoallv-" + str(self.index);
            self.state = Rank.S_COMMUNICATING;
            send_datatype = getDataTypeSize(int(workload[4+2*num_ranks]));
            recv_datatype = getDataTypeSize(int(workload[5+2*num_ranks]));
            send_count = [];
            recv_count = [];
            for i in range(num_ranks):
                send_count.append(int(workload[3 + i]));
                recv_count.append(int(workload[4 + num_ranks + i]));
            self.collective_sr_list = self.col_alltoallv(self.nRanks, self.rank, send_datatype, recv_datatype, send_count, recv_count, self.cycle, rank_offset=0);
            self.counter_waitingCollective = len(self.collective_sr_list[0]);
            return self.collective_sr_list.pop(0);
            
        # ***
        
        
        
        
        
        if(operation == "finalize"):
            self.current_operation = "finalize-" + str(self.index);
            #self.state = Rank.S_ENDED;
            self.shallEnd = True;
            self.state = Rank.S_COMMUNICATING;

            self.collective_sr_list = self.col_barrier(self.nRanks, self.rank, self.cycle, rank_offset=0);
            self.counter_waitingCollective = len(self.collective_sr_list[0]);
            return self.collective_sr_list.pop(0);

        # No blocking operations
        if(operation == "isend"):
            #self.state = Rank.S_COMMUNICATING;
            target=int(workload[2]);
            tag = int(workload[3]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_SEND, self.rank, target, size, self.cycle, MPI_Operations.MPI_ISEND, "isend", blocking = False, tag=tag);
            self.current_operation = "isend-(" + str(target) + ")-" + str(self.index);
            return sr;
        if(operation == "irecv"):
            #self.state = Rank.S_COMMUNICATING;
            source=int(workload[2]);
            tag = int(workload[3]);
            datatype = getDataTypeSize(int(workload[5]));
            size = int(workload[4]) * datatype;
            sr = SendRecv(MPIC_RECV, self.rank, source, size, self.cycle, MPI_Operations.MPI_IRECV, "irecv", blocking = False, tag=tag);
            self.current_operation = "irecv(" + str(source) + ")-" + str(self.index);
            return sr;
        if(operation == "wait"):
            self.current_operation = "wait-" + str(self.index);
            source = int(workload[2]);
            target = int(workload[3]);
            tag = int(workload[4]);
            self.state = Rank.S_WAITING;
            self.waitingTag = tag;
            if(self.check_iSendRecvConclusion(self.waitingTag)):
                self.state = Rank.S_NORMAL;
            return None;
        if(operation == "waitall"):
            self.current_operation = "waitall-" + str(self.index);
            amount = int(workload[2]);
            self.waitingTag = None;
            self.waitall = self.waitall - amount;
            self.state = Rank.S_WAITING;
            return None;

        print( bcolors.FAIL + "ERROR: Unknown operation " + str(operation) + bcolors.ENDC);
        sys.exit(1);



    def __str__ (self):
        return "Rank " + str(self.rank) + " State: " + self.getCurrentStateName() + " WaitCount: " + str(self.waitall) + " Last Command: " + str(self.trace[self.index - 1])