
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


    def __init__(self):
        self.sendQ = []
        self.recvQ = []
        self.matchQ = []

    
    def includeSendRecv(self, sendrecv: SendRecv):
        if sendrecv.kind == MPIC_SEND:
            if not self.checkMatch(sendrecv):
                self.sendQ.append(sendrecv)
        elif sendrecv.kind == MPIC_RECV:
            if not self.checkMatch(sendrecv):
                self.recvQ.append(sendrecv)
        else:
            print( bcolors.FAIL + "ERROR: Unknown SendRecv of kind" + str(sendrecv.kind) + bcolors.ENDC);
            sys.exit(1);
            

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
                match = MQ_Match(sendrecv.rank, partner.rank, partner.size, baseCycle, endCycle);
                self.matchQ.append(match);
                
                return True; # Match!
        return False; # Not a Match!


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

        # Push forward everyone that shares communication with the earliest
        for mi in range( len(self.matchQ) ):
            inc = earliest_match.endCycle - self.matchQ[mi].baseCycle
            if inc > 0:
                self.matchQ[mi].baseCycle = self.matchQ[mi].baseCycle + inc;
                self.matchQ[mi].endCycle = self.matchQ[mi].endCycle + inc;

        return earliest_match;

