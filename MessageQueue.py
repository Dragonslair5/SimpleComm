from email.message import Message
from CollectiveOperationsQueue import *
from Top_FreeMemoryIndependent import TopFreeMemoryIndependent
from Top_NoContention import *
from Topology import *
from Top_CircuitSwitchedFreeMemory import *
from Top_CircuitSwitchedSingleChannel import *
from Top_Kahuna import *
from Top_SharedSingleChannel import *
from Rank import *
from CheckMatch import *



class MessageQueue:


    def __init__(self, numRanks, configfile: SimpleCommConfiguration):
        
        # For debugging purpose
        self.op_message = ""

        # Communication/Contention Topology

        if   configfile.topology == "SC_SHARED":
            self.topology = TopSharedSingleChannel(numRanks, configfile);
        elif configfile.topology == "KAHUNA":
            self.topology = TopKahuna(numRanks, configfile);
        elif configfile.topology == "SC_CS":
            self.topology = TopCircuitSwitchedSingleChannel(numRanks, configfile);
        elif configfile.topology == "FM_CS":
            self.topology = TopCircuitSwitchedFreeMemory(numRanks, configfile);
        elif configfile.topology == "FREE_MEMORY_INDEPENDENT":
            self.topology = TopFreeMemoryIndependent(numRanks, configfile);
        elif configfile.topology == "NO_CONTENTION":
            self.topology = TopNoContention(numRanks, configfile);
        else:
            print( bcolors.FAIL + "ERROR: Unknown topology " + configfile.topology + bcolors.ENDC);
            sys.exit(1);
        
        
        # General SendRecv/Match queue
        self.sendQ: list[SendRecv];
        self.sendQ = [];
        self.recvQ: list[SendRecv];
        self.recvQ = [];
        self.matchQ: list[MQ_Match];
        self.matchQ = [];
        self.matchID = 0; # Counter for setting match ID when creating a MATCH
        self.col_matchQ: list[CollectiveOperationQueueEntry];
        self.col_matchQ = [];

        self.indexAcumulator = [0] * numRanks; # To increment index when including SEND/RECV
        self.currentPosition = [0] * numRanks; # Position for consuming SEND/RECV Matches
        
        # A queue for each collective operation
        self.bcastQ = [];
        self.barrierQ = [];
        self.reduceQ = [];
        self.allreduceQ = [];
        self.alltoallQ = [];
        self.alltoallvQ = [];
        

        self.blockablePendingMessage = [0] * numRanks;

        
        

    
    def includeSendRecv(self, sendrecv: SendRecv):
        
        if sendrecv.blocking:
            self.blockablePendingMessage[sendrecv.rank] = self.blockablePendingMessage[sendrecv.rank] + 1;
            sendrecv.queue_position = self.indexAcumulator[sendrecv.rank]; # Include position of this SendRecv on the MessageQueue
            self.indexAcumulator[sendrecv.rank] = self.indexAcumulator[sendrecv.rank] + 1; # Increment position counter for the next SendRecv of this given rank
        else:
            sendrecv.queue_position = -1;


        if MQ_CheckMatch.checkMatch(sendrecv, self.sendQ, self.recvQ, self.matchQ, self.topology, self.matchID):
            self.matchID = self.matchID + 1;
        else:
            if sendrecv.kind == MPIC_SEND:
                self.sendQ.append(sendrecv)
            elif sendrecv.kind == MPIC_RECV:
                self.recvQ.append(sendrecv)
            else:
                print( bcolors.FAIL + "ERROR: Unknown SendRecv of kind" + str(sendrecv.kind) + bcolors.ENDC);
                sys.exit(1);

        '''
        if sendrecv.kind == MPIC_SEND:
            if not self.checkMatch(sendrecv):
                self.sendQ.append(sendrecv)
            else:
                self.matchID = self.matchID + 1;
        elif sendrecv.kind == MPIC_RECV:
            if not self.checkMatch(sendrecv):
                self.recvQ.append(sendrecv)
            else:
                self.matchID = self.matchID + 1;
        else:
            print( bcolors.FAIL + "ERROR: Unknown SendRecv of kind" + str(sendrecv.kind) + bcolors.ENDC);
            sys.exit(1);
        '''


    def include_Bcast(self, bcast_entry, numRanks) -> None:
        
        #bcast_index = 0;
        # Check if entry for this bcast is already created
        if len(self.bcastQ) > 0: #might be
            for i in range(len(self.bcastQ)):
                if self.bcastQ[i].root == bcast_entry.root:
                    self.bcastQ[i].incEntry(bcast_entry);
                    return None;        
        # Creating a new bcast entry
        root = bcast_entry.root;
        size = bcast_entry.size;
        mqBcast = MQ_Bcast(numRanks, root, size);
        mqBcast.incEntry(bcast_entry);
        self.bcastQ.append(mqBcast);
        return None;

    def include_Barrier(self, barrier_entry, numRanks) -> None:

        # TODO: Expand to multiple communicators
        # Considering only 1 for now
        if len(self.barrierQ) == 0:
            barrier = MQ_Barrier(numRanks);
            self.barrierQ.append(barrier);

        self.barrierQ[0].incEntry(barrier_entry);
        return None;
    
    
    def include_Reduce(self, reduce_entry, numRanks) -> None:
        
        root = reduce_entry.root;
        size = reduce_entry.size;
        
        # TODO: Expand to multiple communicators
        # Considering only 1 for now
        if len(self.reduceQ) == 0:
            reduce = MQ_Reduce(numRanks, root, size);
            self.reduceQ.append(reduce);

        self.reduceQ[0].incEntry(reduce_entry);
        return None;


    def include_Allreduce(self, allreduce_entry, numRanks) -> None:
        size = allreduce_entry.size;

        # TODO: Expand to multiple communicators
        # Considering only 1 for now
        if len(self.allreduceQ) == 0:
            allreduce = MQ_Allreduce(numRanks, size);
            self.allreduceQ.append(allreduce);

        self.allreduceQ[0].incEntry(allreduce_entry);
        return None;

    def include_Alltoall(self, alltoall_entry: MQ_Alltoall_entry, numRanks) -> None:
        sendsize = alltoall_entry.sendsize;
        recvsize = alltoall_entry.recvsize;
        
        # TODO: Expand to multiple communicators
        # Considering only 1 for now
        if len(self.alltoallQ) == 0:
            alltoall = MQ_Alltoall(numRanks, recvsize, sendsize);
            self.alltoallQ.append(alltoall);

        self.alltoallQ[0].incEntry(alltoall_entry);
        return None;

    def include_Alltoallv(self, alltoallv_entry: MQ_Alltoallv_entry, numRanks) -> None:

        # TODO: Expand to multiple communicators
        # Considering only 1 for now
        if len(self.alltoallvQ) == 0:
            alltoallv = MQ_Alltoallv(numRanks);
            self.alltoallvQ.append(alltoallv);

        self.alltoallvQ[0].incEntry(alltoallv_entry);
        return None;


    
    def checkMatch_OLD(self, sendrecv: SendRecv):
        # Look on recvQ or sendQ?
        if sendrecv.kind == MPIC_SEND:
            partner_queue = self.recvQ;
        elif sendrecv.kind == MPIC_RECV:
            partner_queue = self.sendQ;
        else:
            print( bcolors.FAIL + "ERROR: Unknown SendRecv of kind" + str(sendrecv.kind) + bcolors.ENDC);
            sys.exit(1);

        # Try to make a match
        for i in range(len(partner_queue)):
            if ( partner_queue[i].partner == sendrecv.rank and 
                sendrecv.partner == partner_queue[i].rank and
                sendrecv.tag == partner_queue[i].tag ):
                # Grab the matched SendRecv and remove from the queue
                partner: SendRecv;
                partner = partner_queue.pop(i);
                assert sendrecv.tag == partner.tag;

                # Set the baseCycle (the highest between them)
                if sendrecv.baseCycle > partner.baseCycle:
                    baseCycle = sendrecv.baseCycle;
                else:
                    baseCycle = partner.baseCycle;

                latency = 0;
                # Calculate endCycle
                # SEND size must be less or equal to RECV size
                if sendrecv.kind == MPIC_SEND:
                    assert sendrecv.size <= partner.size;
                    #endCycle = baseCycle + SimpleCommunicationCalculus(sendrecv.size);
                    if sendrecv.rank == sendrecv.partner:
                        endCycle = baseCycle + self.topology.SimpleCommunicationCalculusIntranode(sendrecv.size); # inTRA
                        latency = self.topology.intraLatency;
                    else:
                        endCycle = baseCycle + self.topology.SimpleCommunicationCalculusInternode(sendrecv.size); # inTER
                        latency = self.topology.interLatency;
                else:
                    assert sendrecv.size >= partner.size;
                    #endCycle = baseCycle + SimpleCommunicationCalculus(partner.size);
                    if sendrecv.rank == sendrecv.partner:
                        endCycle = baseCycle + self.topology.SimpleCommunicationCalculusIntranode(partner.size); # inTRA
                        latency = self.topology.intraLatency;
                    else:
                        endCycle = baseCycle + self.topology.SimpleCommunicationCalculusInternode(partner.size); # inTER
                        latency = self.topology.interLatency;

                baseCycle = baseCycle + latency; # We consider the latency to be a delay on the start of the communication

                # Create the match and put it on the Matching Queue
                #print("Match " + str())
                assert sendrecv.col_id == partner.col_id, "SEND and RECV have different col_id"
                if sendrecv.kind == MPIC_SEND:
                    match = MQ_Match(self.matchID, sendrecv.rank, partner.rank, sendrecv.size, baseCycle, endCycle, tag = partner.tag, blocking_send=sendrecv.blocking, blocking_recv=partner.blocking, send_origin=sendrecv.operation_origin, recv_origin=partner.operation_origin, positionS=sendrecv.queue_position, positionR=partner.queue_position, latency=latency, col_id=sendrecv.col_id);
                else:
                    match = MQ_Match(self.matchID, partner.rank, sendrecv.rank, partner.size, baseCycle, endCycle, tag = partner.tag, blocking_send=partner.blocking, blocking_recv=sendrecv.blocking, send_origin=partner.operation_origin , recv_origin=sendrecv.operation_origin, positionS=partner.queue_position, positionR=sendrecv.queue_position, latency=latency, col_id=sendrecv.col_id);
                
                #print(match)
                self.matchID = self.matchID + 1;

                # Fulfilling individual information for SEND/RECV to be used by a topology that separates the occurrence of these two
                if sendrecv.kind == MPIC_SEND:
                    match.send_baseCycle = sendrecv.baseCycle;
                    match.send_endCycle = -1;
                    match.still_solving_send = True;

                    match.recv_baseCycle = partner.baseCycle;
                    match.recv_endCycle = -1;

                else:
                    match.send_baseCycle = partner.baseCycle;
                    match.send_endCycle = -1;
                    match.still_solving_send = True;

                    match.recv_baseCycle = sendrecv.baseCycle;
                    match.recv_endCycle = -1;
                # ************



                self.matchQ.append(match);
                
                return True; # Match!
        return False; # Not a Match!


    '''
    # Remember to increment MatchID when this function returns True
    @staticmethod
    def checkMatch(sendrecv: SendRecv,
                   sendQ: typing.List[SendRecv],
                   recvQ: typing.List[SendRecv],
                   matchQ: typing.List[MQ_Match],
                   topology: Topology,
                   matchID: int):

        # Look on recvQ or sendQ?
        if sendrecv.kind == MPIC_SEND:
            partner_queue = recvQ;
        elif sendrecv.kind == MPIC_RECV:
            partner_queue = sendQ;
        else:
            print( bcolors.FAIL + "ERROR: Unknown SendRecv of kind" + str(sendrecv.kind) + bcolors.ENDC);
            sys.exit(1);

        # Try to make a match
        for i in range(len(partner_queue)):
            if ( partner_queue[i].partner == sendrecv.rank and 
                sendrecv.partner == partner_queue[i].rank and
                sendrecv.tag == partner_queue[i].tag ):
                # Grab the matched SendRecv and remove from the queue
                partner: SendRecv;
                partner = partner_queue.pop(i);
                assert sendrecv.tag == partner.tag;

                # Set the baseCycle (the highest between them)
                if sendrecv.baseCycle > partner.baseCycle:
                    baseCycle = sendrecv.baseCycle;
                else:
                    baseCycle = partner.baseCycle;

                latency = 0;
                # Calculate endCycle
                # SEND size must be less or equal to RECV size
                if sendrecv.kind == MPIC_SEND:
                    assert sendrecv.size <= partner.size;
                    #endCycle = baseCycle + SimpleCommunicationCalculus(sendrecv.size);
                    if sendrecv.rank == sendrecv.partner:
                        endCycle = baseCycle + topology.SimpleCommunicationCalculusIntranode(sendrecv.size); # inTRA
                        latency = topology.intraLatency;
                    else:
                        endCycle = baseCycle + topology.SimpleCommunicationCalculusInternode(sendrecv.size); # inTER
                        latency = topology.interLatency;
                else:
                    assert sendrecv.size >= partner.size;
                    #endCycle = baseCycle + SimpleCommunicationCalculus(partner.size);
                    if sendrecv.rank == sendrecv.partner:
                        endCycle = baseCycle + topology.SimpleCommunicationCalculusIntranode(partner.size); # inTRA
                        latency = topology.intraLatency;
                    else:
                        endCycle = baseCycle + topology.SimpleCommunicationCalculusInternode(partner.size); # inTER
                        latency = topology.interLatency;

                baseCycle = baseCycle + latency; # We consider the latency to be a delay on the start of the communication

                # Create the match and put it on the Matching Queue
                #print("Match " + str())
                assert sendrecv.col_id == partner.col_id, "SEND and RECV have different col_id"
                if sendrecv.kind == MPIC_SEND:
                    match = MQ_Match(matchID, sendrecv.rank, partner.rank, sendrecv.size, baseCycle, endCycle, tag = partner.tag, blocking_send=sendrecv.blocking, blocking_recv=partner.blocking, send_origin=sendrecv.operation_origin, recv_origin=partner.operation_origin, positionS=sendrecv.queue_position, positionR=partner.queue_position, latency=latency, col_id=sendrecv.col_id);
                else:
                    match = MQ_Match(matchID, partner.rank, sendrecv.rank, partner.size, baseCycle, endCycle, tag = partner.tag, blocking_send=partner.blocking, blocking_recv=sendrecv.blocking, send_origin=partner.operation_origin , recv_origin=sendrecv.operation_origin, positionS=partner.queue_position, positionR=sendrecv.queue_position, latency=latency, col_id=sendrecv.col_id);
                
                #print(match)
                #matchID = self.matchID + 1;

                # Fulfilling individual information for SEND/RECV to be used by a topology that separates the occurrence of these two
                if sendrecv.kind == MPIC_SEND:
                    match.send_baseCycle = sendrecv.baseCycle;
                    match.send_endCycle = -1;
                    match.still_solving_send = True;

                    match.recv_baseCycle = partner.baseCycle;
                    match.recv_endCycle = -1;

                else:
                    match.send_baseCycle = partner.baseCycle;
                    match.send_endCycle = -1;
                    match.still_solving_send = True;

                    match.recv_baseCycle = sendrecv.baseCycle;
                    match.recv_endCycle = -1;
                # ************



                matchQ.append(match);
                
                return True; # Match!
        return False; # Not a Match!
    '''


    def processCollectiveOperations(self, config: SimpleCommConfiguration) -> None:

        # ******************************************************************
        # bcast (broadcast)
        removal_indexes = []
        for bi in range(len(self.bcastQ)):
            if self.bcastQ[bi].isReady():
                sr_list = self.bcastQ[bi].process(config.CA_Bcast);
                self.op_message = self.op_message + " bcast";
                colMQ = CollectiveOperationQueueEntry(self.blockablePendingMessage, self.indexAcumulator, self.matchID, self.topology);
                #print(sr_list)
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    colMQ.includeSendRecv(sr);
                    #self.includeSendRecv(sr);
                removal_indexes.append(bi);
                self.matchID = colMQ.getMatchID();
                self.col_matchQ.append(colMQ);
        

        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.bcastQ[removal_indexes[i]]
            #self.bcastQ.del(removal_indexes[i]);

        # ******************************************************************
        # barrier (barrier)
        removal_indexes = []
        for bi in range(len(self.barrierQ)):
            if self.barrierQ[bi].isReady():
                sr_list = self.barrierQ[bi].process(config.CA_Barrier);
                self.op_message = self.op_message + " barrier";
                colMQ = CollectiveOperationQueueEntry(self.blockablePendingMessage, self.indexAcumulator, self.matchID, self.topology);
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    colMQ.includeSendRecv(sr);
                    #self.includeSendRecv(sr);
                removal_indexes.append(bi);
                self.matchID = colMQ.getMatchID();
                self.col_matchQ.append(colMQ);

        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.barrierQ[removal_indexes[i]]

        # ******************************************************************
        # reduce (reduce)
        removal_indexes = []
        for ri in range(len(self.reduceQ)):
            if self.reduceQ[ri].isReady():
                sr_list = self.reduceQ[ri].process(config.CA_Reduce);
                self.op_message = self.op_message + " reduce";
                colMQ = CollectiveOperationQueueEntry(self.blockablePendingMessage, self.indexAcumulator, self.matchID, self.topology);
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    colMQ.includeSendRecv(sr);
                    #self.includeSendRecv(sr);
                removal_indexes.append(ri);
                self.matchID = colMQ.getMatchID();
                self.col_matchQ.append(colMQ);
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.reduceQ[removal_indexes[i]]

        # ******************************************************************
        # allreduce (allreduce)
        removal_indexes = []
        for ri in range(len(self.allreduceQ)):
            if self.allreduceQ[ri].isReady():
                sr_list = self.allreduceQ[ri].process(config.CA_Allreduce);
                self.op_message = self.op_message + " allreduce";
                colMQ = CollectiveOperationQueueEntry(self.blockablePendingMessage, self.indexAcumulator, self.matchID, self.topology);
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    #print(sr)
                    colMQ.includeSendRecv(sr);
                    #self.includeSendRecv(sr);
                removal_indexes.append(ri);
                self.matchID = colMQ.getMatchID();
                self.col_matchQ.append(colMQ);
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.allreduceQ[removal_indexes[i]]

        # ******************************************************************
        # alltoall (alltoall)
        removal_indexes = []
        for ai in range(len(self.alltoallQ)):
            if self.alltoallQ[ai].isReady():
                sr_list = self.alltoallQ[ai].process(config.CA_Alltoall);
                self.op_message = self.op_message + " alltoall";
                colMQ = CollectiveOperationQueueEntry(self.blockablePendingMessage, self.indexAcumulator, self.matchID, self.topology);
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    #print(sr)
                    colMQ.includeSendRecv(sr);
                    #self.includeSendRecv(sr);
                removal_indexes.append(ai);
                self.matchID = colMQ.getMatchID();
                self.col_matchQ.append(colMQ);
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.alltoallQ[removal_indexes[i]]

        # ******************************************************************
        # alltoallv (alltoallv)
        removal_indexes = []
        for ai in range(len(self.alltoallvQ)):
            if self.alltoallvQ[ai].isReady():
                sr_list = self.alltoallvQ[ai].process(config.CA_Alltoallv);
                self.op_message = self.op_message + " alltoallv";
                colMQ = CollectiveOperationQueueEntry(self.blockablePendingMessage, self.indexAcumulator, self.matchID, self.topology);
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    #print(sr)
                    colMQ.includeSendRecv(sr);
                    #self.includeSendRecv(sr);
                removal_indexes.append(ai);
                self.matchID = colMQ.getMatchID();
                self.col_matchQ.append(colMQ);
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.alltoallvQ[removal_indexes[i]]



    



    def processMatchQueue(self, list_ranks):
        
        '''
        if len(self.matchQ) == 0 or True:
            for i in range(len(list_ranks)):
                print(list_ranks[i].getCurrentStateName() + " --- " + list_ranks[i].current_operation + " --- Waiting Tag: " + str(list_ranks[i].waitingTag) )
                print("iSendRecv -- ", end= '')
                for l in range(len(list_ranks[i].iSendRecvQ)):
                    print(list_ranks[i].iSendRecvQ[l].tag, end = '');
                    print(" ", end='')
                print("")
        '''
        # Check if anyone is on NORMAL (only process MQ when noone is on NORMAL state)
        for ri in range(len(list_ranks)):
            if list_ranks[ri].state == Rank.S_NORMAL:
                return None;
        
        if len(self.matchQ) == 0:
            for ri in range(len(list_ranks)):
                if list_ranks[ri].state == Rank.S_WAITING:
                    mahTempt = list_ranks[ri].check_iSendRecvConclusion(list_ranks[ri].waitingTag)
                    if mahTempt:
                        print("wtf dude") # This should have been served before (I believe it was already fixed)
        

        #if len(self.matchQ) == 0:
        #    assert len(self.Col_matchQ) > 0, "matchQ == 0 and no Collectives Available on Col_matchQ"
        #    tmp_list = self.Col_matchQ[0].getValidMatches();
        #    if self.Col_matchQ[0].isEmpty():
        #        del self.Col_matchQ[0];
        #    self.matchQ = tmp_list;
            

        #self.processContention(len(list_ranks), self.matchQ, earliest_match, "SC_CC");
        #self.processContention(len(list_ranks), self.matchQ, earliest_match, "SC_FATPIPE");
        earliest_match : MQ_Match;
        earliest_match = self.topology.processContention(self.matchQ, self.col_matchQ, self.currentPosition);

        assert earliest_match is not None, "No match was found on MessageQueue"

        if len(self.col_matchQ) > 0:
            if self.col_matchQ[0].isEmpty():
                del self.col_matchQ[0];


        # Increment position on the queue
        if earliest_match.blocking_send:
            self.currentPosition[earliest_match.rankS] = self.currentPosition[earliest_match.rankS] + 1;
        if earliest_match.blocking_recv:
            self.currentPosition[earliest_match.rankR] = self.currentPosition[earliest_match.rankR] + 1;

        if earliest_match.blocking_send:
            self.blockablePendingMessage[earliest_match.rankS] = self.blockablePendingMessage[earliest_match.rankS] - 1;
        if earliest_match.blocking_recv:
            self.blockablePendingMessage[earliest_match.rankR] = self.blockablePendingMessage[earliest_match.rankR] - 1;


        sending_message = " [" + earliest_match.send_origin + "] S:(";
        receiving_message = ") [" + earliest_match.recv_origin + "] R:(";

        self.op_message = self.op_message + sending_message + str(earliest_match.rankS) + receiving_message + str(earliest_match.rankR) + ") size: " + str(earliest_match.size) + " Bytes" + " Ending in cycle: " + str(earliest_match.endCycle)


        if not self.topology.independent_send_recv:
            earliest_match.send_endCycle = earliest_match.endCycle;
            earliest_match.recv_endCycle = earliest_match.endCycle;


        #print("earliest: ", end='')
        #print(earliest_match)

        return earliest_match;

