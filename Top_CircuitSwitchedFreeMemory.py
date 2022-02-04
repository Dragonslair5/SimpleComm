from Topology import *


class TopCircuitSwitchedFreeMemory(Topology):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopCircuitSwitchedFreeMemory, self).__init__(nRanks, configfile);
        self.nFMUs = configfile.number_of_FMUs;
        assert self.nFMUs > 0, "Number of Free Memory Units needs to be at least 1 when using FMUs topology"



    def processContention_old(self, matchQ, col_matchQ, currentPosition) -> MQ_Match:

        #print("Processing Contention...", end='')
        # We separate the several matches
        valid_matchesQ : list[MQ_Match]; # For valid matches
        valid_matchesQ = []
        invalid_matchesQ : list[MQ_Match]; # For invalid matches
        invalid_matchesQ = []

        ###[1] Find the valid matches
        # Valid matches are the ones that:
        #       1) match their position on the "currentPosition" tracker of the messagequeue 
        #       OR
        #       2) the ones that are untrackable (negative tag)
        # We separate the matches on two arrays, onde for the valid ones (valid_matchesQ)
        # and another for the invalid ondes (invalid_matchesQ)
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

        #print(" " + str(len(valid_matchesQ)) + " / " + str(len(valid_matchesQ) + len(invalid_matchesQ)))

        # find lowest cycle
        lowest_cycle = valid_matchesQ[0].baseCycle;
        li = 0
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle < lowest_cycle:
                lowest_cycle = valid_matchesQ[i].baseCycle;
                li = i

        # Grab the ready match from the matches queue (matchQ) or collectives matches queue (col_matchQ)
        readyMatch = None;
        for j in range(0, len(matchQ)):
            if valid_matchesQ[li].id == matchQ[j].id:
               readyMatch = matchQ.pop(j)
               break;
        if readyMatch is None:
            for j in range(0, len(col_matchQ)):
                readyMatch = col_matchQ[j].getMatchByID(valid_matchesQ[li].id);
                if readyMatch is not None:
                    break;
        # If readyMatch is None, it does not exist... what happened?        
        assert readyMatch is not None, "ready match is not presented on matches queues"

        for j in range(0, len(invalid_matchesQ)):
            if (
                               (readyMatch.rankS == invalid_matchesQ[j].rankS) or
                               (readyMatch.rankS == invalid_matchesQ[j].rankR) or
                               (readyMatch.rankR == invalid_matchesQ[j].rankS) or
                               (readyMatch.rankR == invalid_matchesQ[j].rankR)
            ):
                minToStart = readyMatch.endCycle + invalid_matchesQ[j].latency;
                inc = minToStart - invalid_matchesQ[j].baseCycle;
                
                if inc >= 0:
                    invalid_matchesQ[j].baseCycle = invalid_matchesQ[j].baseCycle + inc;
                    invalid_matchesQ[j].original_baseCycle = invalid_matchesQ[j].original_baseCycle + inc;
                    invalid_matchesQ[j].endCycle = invalid_matchesQ[j].endCycle + inc;

        for j in range(0, len(valid_matchesQ)):
            if (
                               (readyMatch.rankS == valid_matchesQ[j].rankS) or
                               (readyMatch.rankS == valid_matchesQ[j].rankR) or
                               (readyMatch.rankR == valid_matchesQ[j].rankS) or
                               (readyMatch.rankR == valid_matchesQ[j].rankR)
            ):
                minToStart = readyMatch.endCycle + valid_matchesQ[j].latency;
                inc = minToStart - valid_matchesQ[j].baseCycle;
                
                if inc >= 0:
                    valid_matchesQ[j].baseCycle = valid_matchesQ[j].baseCycle + inc;
                    valid_matchesQ[j].original_baseCycle = valid_matchesQ[j].original_baseCycle + inc;
                    valid_matchesQ[j].endCycle = valid_matchesQ[j].endCycle + inc;

        #print("Processing contention complete.")
        return readyMatch;



    def processContention(self, matchQ, col_matchQ, currentPosition) -> MQ_Match:

        # We separate the several matches
        valid_matchesQ : list[MQ_Match]; # For valid matches
        valid_matchesQ = []
        invalid_matchesQ : list[MQ_Match]; # For invalid matches
        invalid_matchesQ = []

        ###[1] Find the valid matches
        # Valid matches are the ones that:
        #       1) match their position on the "currentPosition" tracker of the messagequeue 
        #       OR
        #       2) the ones that are untrackable (negative tag)
        # We separate the matches on two arrays, onde for the valid ones (valid_matchesQ)
        # and another for the invalid ondes (invalid_matchesQ)
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


        # find lowest cycle
        lowest_cycle = valid_matchesQ[0].baseCycle;
        li = 0
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle < lowest_cycle:
                lowest_cycle = valid_matchesQ[i].baseCycle;
                li = i


        # Grab the ready match from the matches queue (matchQ) or collectives matches queue (col_matchQ)
        readyMatch = None;
        for j in range(0, len(matchQ)):
            if valid_matchesQ[li].id == matchQ[j].id:
               readyMatch = matchQ.pop(j)
               break;
        if readyMatch is None:
            for j in range(0, len(col_matchQ)):
                readyMatch = col_matchQ[j].getMatchByID(valid_matchesQ[li].id);
                if readyMatch is not None:
                    break;
        # If readyMatch is None, it does not exist... what happened?        
        assert readyMatch is not None, "ready match is not presented on matches queues"


        for j in range(0, len(invalid_matchesQ)):
            if (
                               (readyMatch.rankS == invalid_matchesQ[j].rankS) or
                               (readyMatch.rankS == invalid_matchesQ[j].rankR) or
                               (readyMatch.rankR == invalid_matchesQ[j].rankS) or
                               (readyMatch.rankR == invalid_matchesQ[j].rankR) or
                               (readyMatch.rankR % self.nFMUs == invalid_matchesQ[j].rankR % self.nFMUs)
            ):
                minToStart = readyMatch.endCycle + invalid_matchesQ[j].latency;
                inc = minToStart - invalid_matchesQ[j].baseCycle;
                
                if inc >= 0:
                    invalid_matchesQ[j].baseCycle = invalid_matchesQ[j].baseCycle + inc;
                    invalid_matchesQ[j].original_baseCycle = invalid_matchesQ[j].original_baseCycle + inc;
                    invalid_matchesQ[j].endCycle = invalid_matchesQ[j].endCycle + inc;

        for j in range(0, len(valid_matchesQ)):
            if (
                               (readyMatch.rankS == valid_matchesQ[j].rankS) or
                               (readyMatch.rankS == valid_matchesQ[j].rankR) or
                               (readyMatch.rankR == valid_matchesQ[j].rankS) or
                               (readyMatch.rankR == valid_matchesQ[j].rankR) or
                               (readyMatch.rankR % self.nFMUs == valid_matchesQ[j].rankR % self.nFMUs)
            ):
                minToStart = readyMatch.endCycle + valid_matchesQ[j].latency;
                inc = minToStart - valid_matchesQ[j].baseCycle;
                
                if inc >= 0:
                    valid_matchesQ[j].baseCycle = valid_matchesQ[j].baseCycle + inc;
                    valid_matchesQ[j].original_baseCycle = valid_matchesQ[j].original_baseCycle + inc;
                    valid_matchesQ[j].endCycle = valid_matchesQ[j].endCycle + inc;

        #print("Processing contention complete.")
        return readyMatch;


        
        