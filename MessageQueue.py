
from Col_Alltoall import MQ_Alltoall
from Col_Alltoallv import MQ_Alltoallv
from Rank import *


# This function is like a placeholder
# TODO: expand this
def SimpleCommunicationCalculus(workload):
    workload = int(workload) + 16
    latency=1;
    bandwidth=1;
    return 10
    return latency + workload/bandwidth;


class MessageQueue:


    def __init__(self, numRanks):
        
        # For debugging purpose
        self.op_message = ""

        # General SendRecv/Match queue
        self.sendQ = [];
        self.recvQ = [];
        self.matchQ = [];

        self.indexAcumulator = [0] * numRanks;
        self.currentPosition = [0] * numRanks;
        
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

        if sendrecv.kind == MPIC_SEND:
            if not self.checkMatch(sendrecv):
                self.sendQ.append(sendrecv)
        elif sendrecv.kind == MPIC_RECV:
            if not self.checkMatch(sendrecv):
                self.recvQ.append(sendrecv)
        else:
            print( bcolors.FAIL + "ERROR: Unknown SendRecv of kind" + str(sendrecv.kind) + bcolors.ENDC);
            sys.exit(1);

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



    def checkMatch(self, sendrecv: SendRecv):
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
                sendrecv.size == partner_queue[i].size ):
                # Grab the matched SendRecv and remove from the queue
                partner: SendRecv;
                partner = partner_queue.pop(i);
                assert sendrecv.tag == partner.tag

                # Set the baseCycle (the highest between them)
                if sendrecv.baseCycle > partner.baseCycle:
                    baseCycle = sendrecv.baseCycle;
                else:
                    baseCycle = partner.baseCycle;

                # Calculate endCycle
                endCycle = baseCycle + SimpleCommunicationCalculus(partner.size);

                # Create the match and put it on the Matching Queue
                #print("Match " + str())
                if sendrecv.kind == MPIC_SEND:
                    match = MQ_Match(sendrecv.rank, partner.rank, partner.size, baseCycle, endCycle, tag = partner.tag, blocking_send=sendrecv.blocking, blocking_recv=partner.blocking, send_origin=sendrecv.operation_origin, recv_origin=partner.operation_origin, positionS=sendrecv.queue_position, positionR=partner.queue_position);
                else:
                    match = MQ_Match(partner.rank, sendrecv.rank, partner.size, baseCycle, endCycle, tag = partner.tag, blocking_send=partner.blocking, blocking_recv=sendrecv.blocking, send_origin=partner.operation_origin , recv_origin=sendrecv.operation_origin, positionS=partner.queue_position, positionR=sendrecv.queue_position);
                
                self.matchQ.append(match);
                
                return True; # Match!
        return False; # Not a Match!


    def processCollectiveOperations(self):

        # ******************************************************************
        # bcast (broadcast)
        removal_indexes = []
        for bi in range(len(self.bcastQ)):
            if self.bcastQ[bi].isReady():
                sr_list = self.bcastQ[bi].process();
                self.op_message = self.op_message + " bcast";
                #print(sr_list)
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    self.includeSendRecv(sr);
                removal_indexes.append(bi);
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.bcastQ[removal_indexes[i]]
            #self.bcastQ.del(removal_indexes[i]);

        # ******************************************************************
        # barrier (barrier)
        removal_indexes = []
        for bi in range(len(self.barrierQ)):
            if self.barrierQ[bi].isReady():
                sr_list = self.barrierQ[bi].process();
                self.op_message = self.op_message + " barrier";
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    self.includeSendRecv(sr);
                removal_indexes.append(bi);

        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.barrierQ[removal_indexes[i]]

        # ******************************************************************
        # reduce (reduce)
        removal_indexes = []
        for ri in range(len(self.reduceQ)):
            if self.reduceQ[ri].isReady():
                sr_list = self.reduceQ[ri].process();
                self.op_message = self.op_message + " reduce";
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    self.includeSendRecv(sr);
                removal_indexes.append(ri);
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.reduceQ[removal_indexes[i]]

        # ******************************************************************
        # allreduce (allreduce)
        removal_indexes = []
        for ri in range(len(self.allreduceQ)):
            if self.allreduceQ[ri].isReady():
                sr_list = self.allreduceQ[ri].process();
                self.op_message = self.op_message + " allreduce";
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    #print(sr)
                    self.includeSendRecv(sr);
                removal_indexes.append(ri);
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.allreduceQ[removal_indexes[i]]

        # ******************************************************************
        # alltoall (alltoall)
        removal_indexes = []
        for ai in range(len(self.alltoallQ)):
            if self.alltoallQ[ai].isReady():
                sr_list = self.alltoallQ[ai].process();
                self.op_message = self.op_message + " alltoall";
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    #print(sr)
                    self.includeSendRecv(sr);
                removal_indexes.append(ai);
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.alltoallQ[removal_indexes[i]]

        # ******************************************************************
        # alltoallv (alltoallv)
        removal_indexes = []
        for ai in range(len(self.alltoallvQ)):
            if self.alltoallvQ[ai].isReady():
                sr_list = self.alltoallvQ[ai].process();
                self.op_message = self.op_message + " alltoallv";
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    #print(sr)
                    self.includeSendRecv(sr);
                removal_indexes.append(ai);
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.alltoallvQ[removal_indexes[i]]



    def processContention(self, matchQ, earliest_match: MQ_Match, topology = "SC_CC"):
        

        if (topology == "SC_CC"):    
            # This is the actual SINGLE CHANNEL CIRCUIT SWITCHING
            # Push forward everyone that shares communication with the earliest
            for mi in range( len(matchQ) ):
                inc = earliest_match.endCycle - matchQ[mi].baseCycle
                if inc > 0:
                    matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                    matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
            return None;
        
        if (topology == "SC_FATPIPE"):
            # Alltoall FATPIPE here
            rank_send = earliest_match.rankS;
            rank_recv = earliest_match.rankR;
            for mi in range( len(matchQ) ):
                if ( (matchQ[mi].rankS - rank_send) * (matchQ[mi].rankS - rank_recv) * (matchQ[mi].rankR - rank_send) * (matchQ[mi].rankR - rank_recv) ) == 0:
                    inc = earliest_match.endCycle - matchQ[mi].baseCycle;
                    matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                    matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
            return None;
            
        print( bcolors.FAIL + "ERROR: Unknown topology " + topology + bcolors.ENDC);
        sys.exit(1);




    def processMatchQueue(self, list_ranks):
        
        # Check if anyone is on NORMAL (only process MQ when noone is on NORMAL state)
        for ri in range(len(list_ranks)):
            if list_ranks[ri].state == Rank.S_NORMAL:
                return None;
        
        # Single channel Circuit Switching
        # ********************************

        #if len(self.matchQ) == 0:
        #    return None;

        # Find the earliest request
        # If this is zero, we might be on a deadlock
        assert len(self.matchQ) > 0, "matchQ is empty on a process queue request"

        index_earliest_request = None;
        lowest_baseCycle = None;
        # Find a valid request for current position
        for i in range(0, len(self.matchQ)):
            thisMatch : MQ_Match = self.matchQ[i];
            #print(str(thisMatch.positionS) + " " + str(self.currentPosition[thisMatch.rankS]) + " | " + str(thisMatch.positionR) + " " + str(self.currentPosition[thisMatch.rankR]));
            if thisMatch.positionS == self.currentPosition[thisMatch.rankS] and thisMatch.positionR == self.currentPosition[thisMatch.rankR]:
                index_earliest_request = i;
                lowest_baseCycle = thisMatch.baseCycle;
                break;

        # We might be on a deadlock if there is no valid match on this point
        #print(*self.matchQ, sep='\n')
        assert index_earliest_request != None, "No valid Match was found"

        # Find the earliest among the valid ones
        #index_earliest_request = 0;
        #lowest_baseCycle = self.matchQ[0].baseCycle;
        for mi in range(0, len(self.matchQ)):
            thisMatch : MQ_Match = self.matchQ[i];
            if thisMatch.baseCycle < lowest_baseCycle and thisMatch.positionS == self.currentPosition[thisMatch.rankS] and thisMatch.positionR == self.currentPosition[thisMatch.rankR]:
                index_earliest_request = mi;
                lowest_baseCycle = self.matchQ[mi].baseCycle;

        # Pop the earliest (and valid) match from the queue
        earliest_match : MQ_Match;
        earliest_match = self.matchQ.pop(index_earliest_request);

        # Increment position on the queue
        self.currentPosition[earliest_match.rankS] = self.currentPosition[earliest_match.rankS] + 1;
        self.currentPosition[earliest_match.rankR] = self.currentPosition[earliest_match.rankR] + 1;


        #self.processContention(self.matchQ, earliest_match, "SC_CC");
        self.processContention(self.matchQ, earliest_match, "SC_FATPIPE");

        '''
        # This is the actual SINGLE CHANNEL CIRCUIT SWITCHING
        # Push forward everyone that shares communication with the earliest
        for mi in range( len(self.matchQ) ):
            inc = earliest_match.endCycle - self.matchQ[mi].baseCycle
            if inc > 0:
                self.matchQ[mi].baseCycle = self.matchQ[mi].baseCycle + inc;
                self.matchQ[mi].endCycle = self.matchQ[mi].endCycle + inc;
        '''

        if earliest_match.blocking_send:
            self.blockablePendingMessage[earliest_match.rankS] = self.blockablePendingMessage[earliest_match.rankS] - 1;
        if earliest_match.blocking_recv:
            self.blockablePendingMessage[earliest_match.rankR] = self.blockablePendingMessage[earliest_match.rankR] - 1;


        sending_message = " [" + earliest_match.send_origin + "] S:(";
        receiving_message = ") [" + earliest_match.recv_origin + "] R:(";

        self.op_message = self.op_message + sending_message + str(earliest_match.rankS) + receiving_message + str(earliest_match.rankR) + ") size: " + str(earliest_match.size) + " Bytes"

        return earliest_match;

