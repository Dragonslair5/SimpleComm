from Rank import *





class NetworkChannel:

    def __init__(self, nRanks, bandwidth, latency):
        self.nRanks = nRanks;
        self.bandwidth = bandwidth;
        self.latency = latency;
        self.lowestCycle = 0;
        self.upperCycle = 0;
        self.activeRanks = [0] * self.nRanks;


    def includeCommunication(self, match : MQ_Match):

        self.activeRanks[match.rankS] = match.endCycle;
        self.activeRanks[match.rankR] = match.endCycle;




class Topology_Kahuna:

    def __init__(self, nRanks, interBW):
        self.nRanks = nRanks;
        self.interBW = interBW;






class Topology:


    def __init__(self, nRanks, topology, interLatency, interBandwidth):
        self.nRanks = nRanks;
        self.topology = topology;
        self.interLatency = interLatency;
        self.interBandwidth = interBandwidth;

    def SimpleCommunicationCalculusInternode(self, workload):
        workload = int(workload) + 16 # 16 Bytes as MPI overhead (based on SimGrid)
        latency=self.interLatency;
        bandwidth=self.interBandwidth;
        return latency + workload/bandwidth;


    def findTheEarliestRequestIndex(self, matchQ, currentPosition) -> int:
        
        index_earliest_request = None;
        lowest_baseCycle = None;
        # Find a valid request for current position
        for i in range(0, len(matchQ)):
            thisMatch : MQ_Match = matchQ[i];
            if (
                (    
                    (thisMatch.positionS == currentPosition[thisMatch.rankS] or thisMatch.positionS < 0) and 
                    (thisMatch.positionR == currentPosition[thisMatch.rankR] or thisMatch.positionR < 0)
                ) or
                (thisMatch.tag < 0)
               ):
                index_earliest_request = i;
                lowest_baseCycle = thisMatch.baseCycle;
                break;

        assert index_earliest_request != None, "No valid Match was found"

        # Find the earliest among the valid ones
        for mi in range(0, len(matchQ)):
            thisMatch : MQ_Match = matchQ[i];
            if (thisMatch.baseCycle < lowest_baseCycle and 
                 (
                    (    
                        (thisMatch.positionS == currentPosition[thisMatch.rankS] or thisMatch.positionS < 0) and 
                        (thisMatch.positionR == currentPosition[thisMatch.rankR] or thisMatch.positionR < 0)
                    ) or
                    (thisMatch.tag < 0)
                 )
               ):
                index_earliest_request = mi;
                lowest_baseCycle = matchQ[mi].baseCycle;

        return index_earliest_request;




    def processContention(self, matchQ, currentPosition):

        #print("Message Queue Size: " + str(len(matchQ)))
        #self.findTheEarliest(matchQ);

        if (self.topology == "SC_SHARED"):

            # If this is zero, we might be on a deadlock
            assert len(matchQ) > 0, "matchQ is empty on a process contention request"

            if len(matchQ) == 1:
                return matchQ.pop(0);

            valid_matchesQ : list[MQ_Match]; # For valid matches
            valid_matchesQ = []

            ###[1] Find the valid matches
            # Valid matches are the ones that match their position on the "currentPosition" tracker of the messagequeue OR
            # the ones that are untrackable (negative tag)
            for i in range(0, len(matchQ)):
                thisMatch : MQ_Match = matchQ[i];
                #print(str(thisMatch.positionS) + " " + str(self.currentPosition[thisMatch.rankS]) + " | " + str(thisMatch.positionR) + " " + str(self.currentPosition[thisMatch.rankR]));
                if (
                    (    
                        (thisMatch.positionS == currentPosition[thisMatch.rankS] or thisMatch.positionS < 0) and 
                        (thisMatch.positionR == currentPosition[thisMatch.rankR] or thisMatch.positionR < 0)
                    ) or
                    (thisMatch.tag < 0)
                ):
                    valid_matchesQ.append(thisMatch)
                    #index_earliest_request = i;
                    #lowest_baseCycle = thisMatch.baseCycle;

            # We might be on a deadlock if there is no valid match on this point
            #print(len(valid_matchesQ))
            assert len(valid_matchesQ) > 0, "No valid Match was found"

            while True:
                # STEP 1 ---- Found Ready?
                # This step is the stop condition in this never-ending loop

                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle == valid_matchesQ[i].endCycle:
                        ready_index = -1;
                        for j in range(0, len(matchQ)):
                            if valid_matchesQ[i].id == matchQ[j].id:
                                ready_index = j;
                                break;
                        assert ready_index != -1, "ready match is not presentes on matches queue"
                        return matchQ.pop(ready_index)
                        return None; # There is one ready to be returned to the rank.
                # ------------------------------------------------------------------

                
                # STEP 2 ---- Find Window
                # lowest_cycle <----> second_lowest_cycle
                lowest_cycle = valid_matchesQ[0].baseCycle;
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < lowest_cycle:
                        lowest_cycle = valid_matchesQ[i].baseCycle;
                second_lowest_cycle = valid_matchesQ[0].endCycle;
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].baseCycle;
                    if valid_matchesQ[i].endCycle < second_lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].endCycle;
                # ------------------------------------------------------------------

                # STEP 3 ---- How Many (share this window)
                window_share_count = 0
                indexes_to_increase = []
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle >= lowest_cycle and valid_matchesQ[i].baseCycle < second_lowest_cycle:
                        window_share_count = window_share_count + 1;
                        indexes_to_increase.append(i)
                        continue
                    if valid_matchesQ[i].endCycle > lowest_cycle and valid_matchesQ[i].endCycle <= second_lowest_cycle:
                        window_share_count = window_share_count + 1;
                        indexes_to_increase.append(i)
                        continue
                    if valid_matchesQ[i].baseCycle < lowest_cycle and valid_matchesQ[i].endCycle > second_lowest_cycle:
                        window_share_count = window_share_count + 1;
                        indexes_to_increase.append(i)
                        continue
                # ------------------------------------------------------------------

                # STEP 4 ---- Increase
                window_size = second_lowest_cycle - lowest_cycle;
                increment = window_size * (window_share_count - 1)
                for i in range(0, len(indexes_to_increase)):
                    curIndex = indexes_to_increase[i];
                    valid_matchesQ[curIndex].baseCycle = valid_matchesQ[curIndex].baseCycle + increment;
                    valid_matchesQ[curIndex].endCycle = valid_matchesQ[curIndex].endCycle + increment;
                lowest_cycle = valid_matchesQ[indexes_to_increase[0]].baseCycle;
                second_lowest_cycle = valid_matchesQ[indexes_to_increase[0]].endCycle;
                # ------------------------------------------------------------------

                # STEP 5 ---- Crop
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].baseCycle;
                    if valid_matchesQ[i].endCycle < second_lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].endCycle;
                
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < second_lowest_cycle:
                        valid_matchesQ[i].baseCycle = second_lowest_cycle;
                # ------------------------------------------------------------------



            print( bcolors.FAIL + "ERROR: Unimplemented topology " + self.topology + bcolors.ENDC);
            sys.exit(1);
            return None


        
        if (self.topology == "KAHUNA"):

            print( bcolors.FAIL + "ERROR: Unimplemented topology " + self.topology + bcolors.ENDC);
            sys.exit(1);
            return None

        if (self.topology == "SC_CC"):

            # Grab the earliest
            earliest_match_index = self.findTheEarliestRequestIndex(matchQ, currentPosition);
            earliest_match = matchQ.pop(earliest_match_index);

            # This is the actual SINGLE CHANNEL CIRCUIT SWITCHING
            # Push forward everyone that shares communication with the earliest
            for mi in range( len(matchQ) ):
                inc = earliest_match.endCycle - matchQ[mi].baseCycle
                if inc > 0:
                    matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                    matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
            return earliest_match;
        
        if (self.topology == "FATPIPE_CCONNODE"):
            
            # Grab the earliest
            earliest_match_index = self.findTheEarliestRequestIndex(matchQ, currentPosition);
            earliest_match = matchQ.pop(earliest_match_index);
            
            # Alltoall FATPIPE here
            rank_send = earliest_match.rankS;
            rank_recv = earliest_match.rankR;
            for mi in range( len(matchQ) ):
                if ( (matchQ[mi].rankS - rank_send) * 
                     (matchQ[mi].rankS - rank_recv) * 
                     (matchQ[mi].rankR - rank_send) * 
                     (matchQ[mi].rankR - rank_recv) ) == 0:
                    inc = earliest_match.endCycle - matchQ[mi].baseCycle;
                    if inc > 0:
                        matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                        matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
                        #if matchQ[mi].removelat:
                        #    matchQ[mi].endCycle = matchQ[mi].endCycle - 1;
                        #    matchQ[mi].removelat = False;
            return earliest_match;

        if (self.topology == "FATPIPE_EXCLUSIVESENDRECVONNODE"):
            
            # Grab the earliest
            earliest_match_index = self.findTheEarliestRequestIndex(matchQ, currentPosition);
            earliest_match = matchQ.pop(earliest_match_index);

            
            # Alltoall FATPIPE here
            rank_send = earliest_match.rankS;
            rank_recv = earliest_match.rankR;
            for mi in range( len(matchQ) ):
                if ( (matchQ[mi].rankS - rank_send) * 
                     (matchQ[mi].rankR - rank_recv) ) == 0:
                    inc = earliest_match.endCycle - matchQ[mi].baseCycle;
                    if inc > 0:
                        matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                        matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
                        #if matchQ[mi].removelat:
                        #    matchQ[mi].endCycle = matchQ[mi].endCycle - 1;
                        #    matchQ[mi].removelat = False;
            return earliest_match;

        if (self.topology == "FATPIPE_FUSEDONNODE"):
            
            # Grab the earliest
            earliest_match_index = self.findTheEarliestRequestIndex(matchQ, currentPosition);
            earliest_match = matchQ.pop(earliest_match_index);
            
            # Alltoall FATPIPE here
            rank_send = earliest_match.rankS;
            rank_recv = earliest_match.rankR;
            for mi in range( len(matchQ) ):
                if ( (matchQ[mi].rankS - rank_send) * 
                     (matchQ[mi].rankS - rank_recv) * 
                     (matchQ[mi].rankR - rank_send) * 
                     (matchQ[mi].rankR - rank_recv) ) == 0:
                    inc = earliest_match.endCycle - matchQ[mi].baseCycle;
                    if inc > 0:
                        matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                        matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
                        earliest_match.endCycle = matchQ[mi].endCycle;
                        #if matchQ[mi].removelat:
                        #    matchQ[mi].endCycle = matchQ[mi].endCycle - 1;
                        #    matchQ[mi].removelat = False;
            return earliest_match;


        if (self.topology == "FATPIPE_FUSEDONNODEV2"):
            # Grab the earliest
            earliest_match_index = self.findTheEarliestRequestIndex(matchQ, currentPosition);
            earliest_match = matchQ.pop(earliest_match_index);
            
            # Alltoall FATPIPE here
            rank_send = earliest_match.rankS;
            rank_recv = earliest_match.rankR;
            for mi in range( len(matchQ) ):
                if ( (matchQ[mi].rankS - rank_send) * 
                     (matchQ[mi].rankS - rank_recv) * 
                     (matchQ[mi].rankR - rank_send) * 
                     (matchQ[mi].rankR - rank_recv) ) == 0:
                    inc = earliest_match.endCycle - matchQ[mi].baseCycle;
                    if inc > 0:
                        #matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                        matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
                        earliest_match.endCycle = earliest_match.endCycle + inc;
                        #if matchQ[mi].removelat:
                        #    matchQ[mi].endCycle = matchQ[mi].endCycle - 1;
                        #    matchQ[mi].removelat = False;
            return earliest_match;


            
            
        print( bcolors.FAIL + "ERROR: Unknown topology " + self.topology + bcolors.ENDC);
        sys.exit(1);

