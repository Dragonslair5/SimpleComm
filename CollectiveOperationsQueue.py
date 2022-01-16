from Rank import *
from Topology import *




class CollectiveOperationQueueEntry:
    def __init__(self, blockablePendingMessage, indexAcumulator, matchID, topology) -> None:
        self.matchQ: list[MQ_Match];
        self.matchQ = [];
        self.sendQ: list[SendRecv];
        self.sendQ = [];
        self.recvQ: list[SendRecv];
        self.recvQ = [];
        self.current_collective_id = 1;
        self.blockablePendingMessage = blockablePendingMessage;
        self.indexAcumulator = indexAcumulator;
        self.matchID = matchID;
        self.topology = topology;

    
    # This function should be called after all the includeSendRecv calls have been done
    # To update the matchID on MessageQueue
    def getMatchID(self) -> int:
        assert len(self.sendQ) == 0, "sendQ not 0 in CollectiveOperationsQueueEntry"
        assert len(self.recvQ) == 0, "recvQ not 0 in CollectiveOperationsQueueEntry"
        return self.matchID;

    def includeSendRecv(self, sendrecv):
        if sendrecv.blocking:
            self.blockablePendingMessage[sendrecv.rank] = self.blockablePendingMessage[sendrecv.rank] + 1;
            sendrecv.queue_position = self.indexAcumulator[sendrecv.rank]; # Include position of this SendRecv on the MessageQueue
            self.indexAcumulator[sendrecv.rank] = self.indexAcumulator[sendrecv.rank] + 1; # Increment position counter for the next SendRecv of this given rank
        else:
            sendrecv.queue_position = -1;

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
                sendrecv.tag == partner_queue[i].tag ):
                # Grab the matched SendRecv and remove from the queue
                partner: SendRecv;
                partner = partner_queue.pop(i);
                assert sendrecv.tag == partner.tag;

                
                #latency = 0;
                #latency = 1.000000
                latency = 0.000008
                #latency = 0.000001
                #latency = 10.000000
                #baseCycle = baseCycle + latency;
                #if sendrecv.kind == MPIC_SEND:
                #    sendrecv.baseCycle = sendrecv.baseCycle + latency;
                #else:
                #    partner.baseCycle = partner.baseCycle + latency;
                
                sendrecv.baseCycle = sendrecv.baseCycle + latency;
                partner.baseCycle = partner.baseCycle + latency;
                # Set the baseCycle (the highest between them)
                if sendrecv.baseCycle > partner.baseCycle:
                    baseCycle = sendrecv.baseCycle;
                else:
                    baseCycle = partner.baseCycle;



                # Calculate endCycle
                # SEND size must be less or equal to RECV size
                if sendrecv.kind == MPIC_SEND:
                    assert sendrecv.size <= partner.size;
                    #endCycle = baseCycle + SimpleCommunicationCalculus(sendrecv.size);
                    if sendrecv.rank == sendrecv.partner:
                        endCycle = baseCycle + self.topology.SimpleCommunicationCalculusIntranode(sendrecv.size); # inTRA
                    else:
                        endCycle = baseCycle + self.topology.SimpleCommunicationCalculusInternode(sendrecv.size); # inTER
                else:
                    assert sendrecv.size >= partner.size;
                    #endCycle = baseCycle + SimpleCommunicationCalculus(partner.size);
                    if sendrecv.rank == sendrecv.partner:
                        endCycle = baseCycle + self.topology.SimpleCommunicationCalculusIntranode(partner.size); # inTRA
                    else:
                        endCycle = baseCycle + self.topology.SimpleCommunicationCalculusInternode(partner.size); # inTER

                # Create the match and put it on the Matching Queue
                #print("Match " + str())
                #print("-----")
                #print(sendrecv)
                #print(partner)
                #print("-----")
                assert sendrecv.col_id == partner.col_id, "SEND and RECV have different col_id"
                if sendrecv.kind == MPIC_SEND:
                    match = MQ_Match(self.matchID, sendrecv.rank, partner.rank, partner.size, baseCycle, endCycle, tag = partner.tag, blocking_send=sendrecv.blocking, blocking_recv=partner.blocking, send_origin=sendrecv.operation_origin, recv_origin=partner.operation_origin, positionS=sendrecv.queue_position, positionR=partner.queue_position, latency=latency, col_id=sendrecv.col_id);
                else:
                    match = MQ_Match(self.matchID, partner.rank, sendrecv.rank, partner.size, baseCycle, endCycle, tag = partner.tag, blocking_send=partner.blocking, blocking_recv=sendrecv.blocking, send_origin=partner.operation_origin , recv_origin=sendrecv.operation_origin, positionS=partner.queue_position, positionR=sendrecv.queue_position, latency=latency, col_id=sendrecv.col_id);
                
                self.matchID = self.matchID + 1;

                self.matchQ.append(match);
                
                return True; # Match!
        return False; # Not a Match!


    def getValidAndInvalidMatches(self) -> typing.Tuple[ typing.List[MQ_Match] , typing.List[MQ_Match] ]:
        #print("Col MatchQ size: " + str(len(self.matchQ)))
        valid_matches = [];
        invalid_matches = [];
        lowest_col_id = self.matchQ[0].col_id;
        for i in range(0, len(self.matchQ)):
            if lowest_col_id > self.matchQ[i].col_id:
                lowest_col_id = self.matchQ[i].col_id;
        for i in range(0, len(self.matchQ)):
            if self.matchQ[i].col_id == lowest_col_id:
                valid_matches.append(self.matchQ[i]);
            else:
                invalid_matches.append(self.matchQ[i]);
        return valid_matches, invalid_matches;

    def getMatchByID(self, id: int) -> MQ_Match:
        #print("xXxxXxXxXxXxXxXxXxXxXxXxXXXXXXXXx --- " + str(id))
        #for i in range(0, len(self.matchQ)):
        #    print(str(self.matchQ[i].id) + " ", end='')
        #print("")
        #print("xXxxXxXxXxXxXxXxXxXxXxXxXXXXXXXXx")
        for i in range(0, len(self.matchQ)):
            if self.matchQ[i].id == id:
                return self.matchQ.pop(i);
        return None;

    # This should be used after calling getValidMatches
    #def getInvalidMatches(self) -> typing.List[MQ_Match]:
    #    return self.matchQ;

    def isEmpty(self) -> bool:
        if len(self.matchQ) == 0:
            return True;
        else:
            return False;