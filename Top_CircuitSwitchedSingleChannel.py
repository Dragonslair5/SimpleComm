from Topology import *


class TopCircuitSwitchedSingleChannel(Topology):


    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopCircuitSwitchedSingleChannel, self).__init__(nRanks, configfile);
        assert self.eager_protocol_max_size == 0, "Eager Protocol can not be activated with this Topology"


    def processContention(self, matchQ, col_matchQ, currentPosition) -> MQ_Match:
        
        valid_matchesQ : list[MQ_Match]; # For valid matches
        valid_matchesQ = []
        invalid_matchesQ : list[MQ_Match]; # For invalid matches
        invalid_matchesQ = []
        ###[1] Find the valid matches
        # Valid matches are the ones that:
        #       1) match their position on the "currentPosition" tracker of the messagequeue 
        #       OR
        #       2) the ones that are untrackable (negative tag)
        for i in range(0, len(matchQ)):
            thisMatch : MQ_Match = matchQ[i];
            
            if (
                (    
                    (thisMatch.positionS == currentPosition[thisMatch.rankS] or thisMatch.positionS < 0) and 
                    (thisMatch.positionR == currentPosition[thisMatch.rankR] or thisMatch.positionR < 0)
                ) or
                (thisMatch.tag < 0)
            ):
                valid_matchesQ.append(thisMatch)
            else:
                invalid_matchesQ.append(thisMatch);
        
        # Valid among Collectives
        for i in range(0, len(col_matchQ)):
            tmp_valid, tmp_invalid = col_matchQ[i].getValidAndInvalidMatches();
            valid_matchesQ = valid_matchesQ + tmp_valid;
            invalid_matchesQ = invalid_matchesQ + tmp_invalid;
        # We might be on a deadlock if there is no valid match on this point
        assert len(valid_matchesQ) > 0, "No valid Match was found"
        
        
        # Grab the earliest
        earliest_match_index = self.findTheEarliestRequestIndex(valid_matchesQ, currentPosition);
        
        readyMatch = None;
        for j in range(0, len(matchQ)):
            if valid_matchesQ[earliest_match_index].id == matchQ[j].id:
               readyMatch = matchQ.pop(j)
               break;
        if readyMatch is None:
            for j in range(0, len(col_matchQ)):
                readyMatch = col_matchQ[j].getMatchByID(valid_matchesQ[earliest_match_index].id);
                if readyMatch is not None:
                    break;
        # If readyMatch is None, it does not exist... what happened?        
        assert readyMatch is not None, "ready match is not presented on matches queues"
        
        # This is the actual SINGLE CHANNEL CIRCUIT SWITCHING
        # Push forward everyone that shares communication with the earliest
        for mi in range( len(matchQ) ):
            inc = readyMatch.endCycle - matchQ[mi].baseCycle
            if inc > 0:
                matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
        
        return readyMatch;