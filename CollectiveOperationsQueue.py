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
                #print("-----")
                #print(sendrecv)
                #print(partner)
                #print("-----")
                assert sendrecv.col_id == partner.col_id, "SEND and RECV have different col_id"
                if sendrecv.kind == MPIC_SEND:
                    match = MQ_Match(self.matchID, sendrecv.rank, partner.rank, sendrecv.size, baseCycle, endCycle, tag = partner.tag, blocking_send=sendrecv.blocking, blocking_recv=partner.blocking, send_origin=sendrecv.operation_origin, recv_origin=partner.operation_origin, positionS=sendrecv.queue_position, positionR=partner.queue_position, latency=latency, col_id=sendrecv.col_id);
                else:
                    match = MQ_Match(self.matchID, partner.rank, sendrecv.rank, partner.size, baseCycle, endCycle, tag = partner.tag, blocking_send=partner.blocking, blocking_recv=sendrecv.blocking, send_origin=partner.operation_origin , recv_origin=sendrecv.operation_origin, positionS=partner.queue_position, positionR=sendrecv.queue_position, latency=latency, col_id=sendrecv.col_id);
                
                self.matchID = self.matchID + 1;

                # Fulfilling individual information for SEND/RECV to be used by a topology that separates the occurrence of these two
                if sendrecv.kind == MPIC_SEND:
                    match.send_original_baseCycle = sendrecv.baseCycle;
                    match.send_baseCycle = sendrecv.baseCycle;
                    match.send_endCycle = -1;
                    match.still_solving_send = True;

                    match.recv_original_baseCycle = partner.baseCycle;
                    match.recv_baseCycle = partner.baseCycle;
                    match.recv_endCycle = -1;

                else:
                    match.send_original_baseCycle = partner.baseCycle;
                    match.send_baseCycle = partner.baseCycle;
                    match.send_endCycle = -1;
                    match.still_solving_send = True;

                    match.recv_original_baseCycle = sendrecv.baseCycle;
                    match.recv_baseCycle = sendrecv.baseCycle;
                    match.recv_endCycle = -1;
                # ************

                self.matchQ.append(match);
                
                return True; # Match!
        return False; # Not a Match!


    def getValidAndInvalidMatches(self) -> typing.Tuple[ typing.List[MQ_Match] , typing.List[MQ_Match] ]:
        #print("Col MatchQ size: " + str(len(self.matchQ)))
        assert len(self.matchQ) > 0, "Where are the matches?"
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