
from tp_utils import *
from MPI_Constants import *
from Rank import *


# This function is like a placeholder
# TODO: expand this
def SimpleCommunicationCalculus(workload):
    workload = int(workload)
    latency=1;
    bandwidth=1;
    return latency + workload/bandwidth;


class MessageQueue:


    def __init__(self, numRanks):
        self.sendQ = [];
        self.recvQ = [];
        self.matchQ = [];
        self.bcastQ = [];

        self.blockablePendingMessage = [0] * numRanks;
        

    
    def includeSendRecv(self, sendrecv: SendRecv):
        
        self.blockablePendingMessage[sendrecv.rank] = self.blockablePendingMessage[sendrecv.rank] + 1;

        if sendrecv.kind == MPIC_SEND:
            if not self.checkMatch(sendrecv):
                self.sendQ.append(sendrecv)
        elif sendrecv.kind == MPIC_RECV:
            if not self.checkMatch(sendrecv):
                self.recvQ.append(sendrecv)
        else:
            print( bcolors.FAIL + "ERROR: Unknown SendRecv of kind" + str(sendrecv.kind) + bcolors.ENDC);
            sys.exit(1);

    def include_Bcast(self, bcast_entry, numRanks):
        
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
                partner = partner_queue.pop(i);

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
                    match = MQ_Match(sendrecv.rank, partner.rank, partner.size, baseCycle, endCycle);
                else:
                    match = MQ_Match(partner.rank, sendrecv.rank, partner.size, baseCycle, endCycle);
                
                self.matchQ.append(match);
                
                return True; # Match!
        return False; # Not a Match!


    def processCollectiveOperations(self):

        # bcast (broadcast)
        removal_indexes = []
        for bi in range(len(self.bcastQ)):
            if self.bcastQ[bi].isReady():
                sr_list = self.bcastQ[bi].process();
                #print(sr_list)
                while len(sr_list) > 0:
                    sr = sr_list.pop(0);
                    self.includeSendRecv(sr);
                removal_indexes.append(bi)
        
        for i in range(len(removal_indexes)-1, -1, -1):
            #print("Removing " + str(removal_indexes[i]) )
            del self.bcastQ[removal_indexes[i]]
            #self.bcastQ.del(removal_indexes[i]);





    def processMatchQueue(self, list_ranks):
        
        # Check if anyone is on NORMAL (only process MQ when noone is on NORMAL state)
        for ri in range(len(list_ranks)):
            if list_ranks[ri].state == Rank.S_NORMAL:
                return None;
        
        # Single channel Circuit Switching
        # ********************************

        # Find the earliest request
        # If it is zero, we might be on a deadlock
        assert len(self.matchQ) > 0, "matchQ is empty on a process queue request"
        index_earliest_request = 0;
        lowest_baseCycle = self.matchQ[0].baseCycle;
        for mi in range(1, len(self.matchQ)):
            if self.matchQ[mi].baseCycle < lowest_baseCycle:
                index_earliest_request = mi;
                lowest_baseCycle = self.matchQ[mi].baseCycle;

        # Pop the earliest match from the queue
        earliest_match = self.matchQ.pop(index_earliest_request);

        # This is the actual SINGLE CHANNEL CIRCUIT SWITCHING
        # Push forward everyone that shares communication with the earliest
        for mi in range( len(self.matchQ) ):
            inc = earliest_match.endCycle - self.matchQ[mi].baseCycle
            if inc > 0:
                self.matchQ[mi].baseCycle = self.matchQ[mi].baseCycle + inc;
                self.matchQ[mi].endCycle = self.matchQ[mi].endCycle + inc;


        self.blockablePendingMessage[earliest_match.rankS] = self.blockablePendingMessage[earliest_match.rankS] - 1;
        self.blockablePendingMessage[earliest_match.rankR] = self.blockablePendingMessage[earliest_match.rankR] - 1;

        return earliest_match;

