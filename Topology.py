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

    def SimpleCommunicationCalculusIntranode(self, workload):
        workload = int(workload) + 16 # 16 Bytes as MPI overhead (based on SimGrid)
        latency=self.interLatency;
        bandwidth=self.interBandwidth;
        return 0;
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
            #print("******")
            #print(matchQ)
            #print("******")

            if len(matchQ) == 1:
                return matchQ.pop(0);

            valid_matchesQ : list[MQ_Match]; # For valid matches
            valid_matchesQ = []
            invalid_matchesQ : list[MQ_Match]; # For invalid matches
            invalid_matchesQ = []

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
                else:
                    invalid_matchesQ.append(thisMatch);
                    #index_earliest_request = i;
                    #lowest_baseCycle = thisMatch.baseCycle;

            # We might be on a deadlock if there is no valid match on this point
            #print("-----")
            #print(valid_matchesQ)
            #print("-----")
            #print(len(valid_matchesQ))
            assert len(valid_matchesQ) > 0, "No valid Match was found"

            while True:
                #print("*****************************")
                # STEP 1 ---- Found Ready?
                # This step is the stop condition in this never-ending loop

                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle == valid_matchesQ[i].endCycle:
                        ready_index = -1;
                        for j in range(0, len(matchQ)):
                            if valid_matchesQ[i].id == matchQ[j].id:
                                ready_index = j;
                                break;
                        assert ready_index != -1, "ready match is not presented on matches queue"

                        for j in range(0, len(invalid_matchesQ)):
                            if (
                               (valid_matchesQ[i].send_origin == invalid_matchesQ[j].send_origin) or
                               (valid_matchesQ[i].send_origin == invalid_matchesQ[j].recv_origin) or
                               (valid_matchesQ[i].recv_origin == invalid_matchesQ[j].send_origin) or
                               (valid_matchesQ[i].recv_origin == invalid_matchesQ[j].recv_origin)
                            ):
                               inc = valid_matchesQ[i].endCycle - invalid_matchesQ[j].baseCycle;
                               if inc > 0:
                                   invalid_matchesQ[j].baseCycle = invalid_matchesQ[j].baseCycle + inc;
                                   invalid_matchesQ[j].endCycle = invalid_matchesQ[j].endCycle + inc

                        return matchQ.pop(ready_index)
                        return None; # There is one ready to be returned to the rank.
                # ------------------------------------------------------------------

                
                # STEP 2 ---- Find Window
                # lowest_cycle <----> second_lowest_cycle
                lowest_cycle = valid_matchesQ[0].baseCycle;
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < lowest_cycle:
                        lowest_cycle = valid_matchesQ[i].baseCycle;
                
                second_lowest_cycle = valid_matchesQ[0].getUpperCycle();
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].baseCycle;
                    if valid_matchesQ[i].getUpperCycle() < second_lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].getUpperCycle();
                # ------------------------------------------------------------------
                #print(str(lowest_cycle) + " ---- " + str(second_lowest_cycle))

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
                #print(window_share_count)
                #print(indexes_to_increase)
                # STEP 4 ---- Increase
                window_size = second_lowest_cycle - lowest_cycle;
                newFactor = window_share_count;
                #increment = window_size * (window_share_count - 1)
                increment_list = []
                for i in range(0, len(indexes_to_increase)):
                    curIndex = indexes_to_increase[i];
                    currentFactor = valid_matchesQ[curIndex].bw_factor;
                    increment = (window_size * (newFactor / currentFactor) ) - window_size;
                    increment_list.append(increment);
                #increment_list.sort();
                #print(increment_list)
                smallest_increment = increment_list[0];
                for i in range(1, len(increment_list)):
                    if increment_list[i] < smallest_increment:
                        smallest_increment = increment_list[i];
                #assert smallest_increment > 0, "Increment cannot be zero (0)";
                for i in range(0, len(indexes_to_increase)):
                    curIndex = indexes_to_increase[i];
                    increment = increment_list[i];
                    flooded_increment = 0;
                    if increment > smallest_increment:
                        flooded_increment = (increment - smallest_increment) * 1 / newFactor;
                    
                    valid_matchesQ[curIndex].baseCycle = valid_matchesQ[curIndex].baseCycle + window_size;
                    valid_matchesQ[curIndex].endCycle = valid_matchesQ[curIndex].endCycle + smallest_increment + flooded_increment;
                    valid_matchesQ[curIndex].solvedCycle = valid_matchesQ[curIndex].baseCycle + smallest_increment;
                    valid_matchesQ[curIndex].bw_factor = newFactor;

                    #valid_matchesQ[curIndex].baseCycle = valid_matchesQ[curIndex].baseCycle + increment;
                    #valid_matchesQ[curIndex].endCycle = valid_matchesQ[curIndex].endCycle + increment;
                lowest_cycle = valid_matchesQ[indexes_to_increase[0]].baseCycle;
                second_lowest_cycle = valid_matchesQ[indexes_to_increase[0]].getUpperCycle();
                # ------------------------------------------------------------------

                # STEP 5 ---- Crop
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].baseCycle;
                    if valid_matchesQ[i].getUpperCycle() < second_lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].getUpperCycle();
                
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < second_lowest_cycle:
                        valid_matchesQ[i].baseCycle = second_lowest_cycle;
                    if valid_matchesQ[i].solvedCycle == second_lowest_cycle:
                        valid_matchesQ[i].solvedCycle = -1;
                        valid_matchesQ[i].bw_factor = 1;
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

