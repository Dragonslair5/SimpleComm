from Top_FreeMemoryUnit import *
from Topology import *
from Top_Kahuna import *
from Top_Hybrid import *
from Rank import *
from CheckMatch import *



class MessageQueue:


    def __init__(self, numRanks, configfile: SimpleCommConfiguration):
        
        # For debugging purpose
        self.op_message = ""

        # Communication/Contention Topology

        if configfile.topology == "KAHUNA": # *** KAHUNA
            self.topology = TopKahuna(numRanks, configfile);
        elif configfile.topology == "HYBRID": # *** HYBRID
            self.topology = TopHybrid(numRanks, configfile);
        elif configfile.topology == "FMU": # *** FMU
            self.topology = TopFreeMemoryUnit(numRanks, configfile);
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

        self.indexAcumulator = [0] * numRanks; # To increment index when including SEND/RECV
        self.currentPosition = [0] * numRanks; # Position for consuming SEND/RECV Matches
        
        self.blockablePendingMessage = [0] * numRanks;

        # Modifiers
        self.boosterFactor = configfile.booster_factor;
        self.use_booster_factor_every = configfile.use_booster_factor_every;
        self.use_booster_factor_counter = 0;


    def shouldUseBoosterFactor(self)->bool:
        if self.use_booster_factor_counter % self.use_booster_factor_every == 0:
            return True;
        else:
            return False;


    
    def includeSendRecv(self, sendrecv: SendRecv):
        
        if sendrecv.blocking:
            self.blockablePendingMessage[sendrecv.rank] = self.blockablePendingMessage[sendrecv.rank] + 1;
            sendrecv.queue_position = self.indexAcumulator[sendrecv.rank]; # Include position of this SendRecv on the MessageQueue
            self.indexAcumulator[sendrecv.rank] = self.indexAcumulator[sendrecv.rank] + 1; # Increment position counter for the next SendRecv of this given rank
        else:
            sendrecv.queue_position = -1;


        if MQ_CheckMatch.checkMatch(sendrecv, self.sendQ, self.recvQ, self.matchQ, self.topology, self.matchID, self.boosterFactor, self.shouldUseBoosterFactor()):
            self.matchID = self.matchID + 1;
            self.use_booster_factor_counter = (self.use_booster_factor_counter + 1) % self.use_booster_factor_every;
        else:
            if sendrecv.kind == MPIC_SEND:
                self.sendQ.append(sendrecv)
            elif sendrecv.kind == MPIC_RECV:
                self.recvQ.append(sendrecv)
            else:
                print( bcolors.FAIL + "ERROR: Unknown SendRecv of kind" + str(sendrecv.kind) + bcolors.ENDC);
                sys.exit(1);
        
        #print("senQ: " + str(len(self.sendQ)) + " recvQ: " + str(len(self.recvQ)) + " matchQ: " + str(len(self.matchQ)) )

    



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
        earliest_match = self.topology.processContention(self.matchQ);

        assert earliest_match is not None, "No match was found on MessageQueue"

        #if len(self.col_matchQ) > 0:
        #    if self.col_matchQ[0].isEmpty():
        #        del self.col_matchQ[0];


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
        if isinstance(self.topology, TopFreeMemoryUnit):
            self.op_message = self.op_message + " fmu: " + str(earliest_match.fmu_in_use);

        if not self.topology.independent_send_recv:
            earliest_match.send_endCycle = earliest_match.endCycle;
            earliest_match.recv_endCycle = earliest_match.endCycle;

            # Eager Protocol
            if earliest_match.size < self.topology.eager_protocol_max_size:
                earliest_match.send_endCycle = earliest_match.send_baseCycle;
                if earliest_match.recv_baseCycle > earliest_match.endCycle:
                    earliest_match.recv_endCycle = earliest_match.recv_baseCycle;
        else:
            # Eager Protocol
            if earliest_match.size < self.topology.eager_protocol_max_size:
                earliest_match.send_endCycle = earliest_match.send_baseCycle;

        #print("earliest: ", end='')
        #print(earliest_match)

        return earliest_match;

