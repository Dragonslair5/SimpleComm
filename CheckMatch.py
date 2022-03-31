

from MPI_Constants import *
from SendRecv import *
from Topology import *



class MQ_CheckMatch:

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
                bandwidth = 1;
                # Calculate endCycle
                # SEND size must be less or equal to RECV size
                if sendrecv.kind == MPIC_SEND:
                    assert sendrecv.size <= partner.size;

                    # Eager Protocol
                    if sendrecv.size < topology.eager_protocol_max_size:
                        baseCycle = sendrecv.baseCycle;

                    #endCycle = baseCycle + SimpleCommunicationCalculus(sendrecv.size);
                    if sendrecv.rank == sendrecv.partner:
                        #endCycle = baseCycle + topology.SimpleCommunicationCalculusIntranode(sendrecv.size); # inTRA
                        latency = topology.intraLatency;
                    else:
                        #endCycle = baseCycle + topology.SimpleCommunicationCalculusInternode(sendrecv.size); # inTER
                        latency = topology.interLatency;
                    endCycle, bandwidth = topology.CommunicationCalculus_Bandwidth(sendrecv.rank, sendrecv.partner, sendrecv.size);
                    endCycle = endCycle + baseCycle;
                    #endCycle = baseCycle + topology.CommunicationCalculus_Bandwidth(sendrecv.rank, sendrecv.partner, sendrecv.size)
                else:
                    assert sendrecv.size >= partner.size;

                    # Eager Protocol
                    if partner.size < topology.eager_protocol_max_size:
                        baseCycle = partner.baseCycle;

                    #endCycle = baseCycle + SimpleCommunicationCalculus(partner.size);
                    if sendrecv.rank == sendrecv.partner:
                        #endCycle = baseCycle + topology.SimpleCommunicationCalculusIntranode(partner.size); # inTRA
                        latency = topology.intraLatency;
                    else:
                        #endCycle = baseCycle + topology.SimpleCommunicationCalculusInternode(partner.size); # inTER
                        latency = topology.interLatency;
                    endCycle, bandwidth = topology.CommunicationCalculus_Bandwidth(partner.rank, partner.partner, partner.size);
                    endCycle = endCycle + baseCycle;
                    #endCycle = baseCycle + topology.CommunicationCalculus_Bandwidth(partner.rank, partner.partner, partner.size)

                # We consider the latency to be a delay on the start of the communication
                baseCycle = baseCycle + latency; 
                endCycle = endCycle + latency; 

                # Create the match and put it on the Matching Queue
                #print("Match " + str())
                assert sendrecv.col_id == partner.col_id, "SEND and RECV have different col_id"
                if sendrecv.kind == MPIC_SEND:
                    match = MQ_Match(matchID, sendrecv.rank, partner.rank, sendrecv.size, baseCycle, endCycle, tag = partner.tag, blocking_send=sendrecv.blocking, blocking_recv=partner.blocking, send_origin=sendrecv.operation_origin, recv_origin=partner.operation_origin, positionS=sendrecv.queue_position, positionR=partner.queue_position, bandwidth=bandwidth, latency=latency, col_id=sendrecv.col_id);
                else:
                    match = MQ_Match(matchID, partner.rank, sendrecv.rank, partner.size, baseCycle, endCycle, tag = partner.tag, blocking_send=partner.blocking, blocking_recv=sendrecv.blocking, send_origin=partner.operation_origin , recv_origin=sendrecv.operation_origin, positionS=partner.queue_position, positionR=sendrecv.queue_position, bandwidth=bandwidth, latency=latency, col_id=sendrecv.col_id);
                
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