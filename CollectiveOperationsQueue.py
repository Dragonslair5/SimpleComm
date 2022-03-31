from Rank import *
from Topology import *
from CheckMatch import *




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