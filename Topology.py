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

# *****************************************************************************
#    ____   ____     ____  _   _    _    ____  _____ ____  
#   / ___| / ___|   / ___|| | | |  / \  |  _ \| ____|  _ \ 
#   \___ \| |       \___ \| |_| | / _ \ | |_) |  _| | | | |
#    ___) | |___     ___) |  _  |/ ___ \|  _ <| |___| |_| |
#   |____/ \____|___|____/|_| |_/_/   \_\_| \_\_____|____/ 
#              |_____|                                                                
# *****************************************************************************
    def __alg_SC_SHARED(self, matchQ, col_matchQ, currentPosition):
            
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

            while True:
                #print(bcolors.OKBLUE + "********************************************************" + bcolors.ENDC)
                #print(bcolors.OKGREEN + "STEP 1 ---- Found Ready?" + bcolors.ENDC)
                # STEP 1 ---- Found Ready?
                # This step is the stop condition in this never-ending loop
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle == valid_matchesQ[i].endCycle:
                        #print("READY")
                        #print(bcolors.OKCYAN, end='');
                        #print(valid_matchesQ[i])
                        print(valid_matchesQ[i].data_sent)
                        #print(bcolors.ENDC, end='');
                        readyMatch = None;
                        for j in range(0, len(matchQ)):
                            if valid_matchesQ[i].id == matchQ[j].id:
                               readyMatch = matchQ.pop(j)
                               break;
                        if readyMatch is None:
                            for j in range(0, len(col_matchQ)):
                                readyMatch = col_matchQ[j].getMatchByID(valid_matchesQ[i].id);
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
                                   
                        return readyMatch;
                # ------------------------------------------------------------------

                #print(bcolors.OKGREEN + "STEP 2 ---- Find Window" + bcolors.ENDC)
                # STEP 2 ---- Find Window
                # lowest_cycle <----> second_lowest_cycle
                li: int; 
                sli: int;
                lowest_cycle = valid_matchesQ[0].baseCycle;
                li = valid_matchesQ[0].id;
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < lowest_cycle:
                        lowest_cycle = valid_matchesQ[i].baseCycle;
                        li = valid_matchesQ[i].id
                
                second_lowest_cycle = valid_matchesQ[0].getUpperCycle();
                sli=valid_matchesQ[0].id;
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].baseCycle;
                        sli=valid_matchesQ[i].id;
                    if valid_matchesQ[i].getUpperCycle() < second_lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].getUpperCycle();
                        sli=valid_matchesQ[i].id*-1
                # ------------------------------------------------------------------

                #print(bcolors.OKGREEN + "STEP 3 ---- How Many (share this window)" + bcolors.ENDC)
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

                #print(bcolors.OKGREEN + "STEP 4 ---- Increase" + bcolors.ENDC)
                # STEP 4 ---- Increase
                window_size = second_lowest_cycle - lowest_cycle;
                newFactor = window_share_count;
                increment_list = []
                for i in range(0, len(indexes_to_increase)):
                    curIndex = indexes_to_increase[i];
                    currentFactor = valid_matchesQ[curIndex].bw_factor;
                    increment = (window_size * (newFactor / currentFactor) ) - window_size;
                    assert increment >= 0, "increment can not be negative"
                    increment_list.append(increment);
                smallest_increment = increment_list[0];
                for i in range(1, len(increment_list)):
                    if increment_list[i] < smallest_increment:
                        smallest_increment = increment_list[i];
                for i in range(0, len(indexes_to_increase)):
                    curIndex = indexes_to_increase[i];
                    increment = increment_list[i];
                    flooded_increment = 0;
                    if increment > smallest_increment:
                        flooded_increment = (increment - smallest_increment) * 1 / newFactor;
                        assert flooded_increment >= 0, "flooded increment can not be negative"
                    if valid_matchesQ[curIndex].solvedCycle != -1:
                        decrement = valid_matchesQ[curIndex].solvedCycle - (valid_matchesQ[curIndex].baseCycle + window_size)
                        decrement = decrement - decrement * 1/valid_matchesQ[curIndex].bw_factor
                        valid_matchesQ[curIndex].endCycle = valid_matchesQ[curIndex].endCycle - decrement
                    valid_matchesQ[curIndex].endCycle = valid_matchesQ[curIndex].endCycle + smallest_increment + flooded_increment;
                    valid_matchesQ[curIndex].solvedCycle = valid_matchesQ[curIndex].baseCycle + window_size + smallest_increment;
                    valid_matchesQ[curIndex].bw_factor = newFactor;
                # ------------------------------------------------------------------

                #print(bcolors.OKGREEN + "STEP 5 ---- Crop" + bcolors.ENDC)
                # STEP 5 ---- Crop
                lci = valid_matchesQ[indexes_to_increase[0]].id;
                lowest_cycle = valid_matchesQ[indexes_to_increase[0]].baseCycle;
                slci = lci
                second_lowest_cycle = valid_matchesQ[indexes_to_increase[0]].getUpperCycle();                
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
                        second_lowest_cycle = valid_matchesQ[i].baseCycle;
                        slci = valid_matchesQ[i].id;
                    if valid_matchesQ[i].getUpperCycle() < second_lowest_cycle:
                        assert False, "Mah que que eh isso!?" # Is it possible to get here?
                        second_lowest_cycle = valid_matchesQ[i].getUpperCycle();

                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < second_lowest_cycle:
                        #valid_matchesQ[i].transmitted_data.append([second_lowest_cycle - valid_matchesQ[i].baseCycle, valid_matchesQ[i].bw_factor, (2500000000/valid_matchesQ[i].bw_factor)*(second_lowest_cycle - valid_matchesQ[i].baseCycle) ])
                        valid_matchesQ[i].includeTransmittedData(second_lowest_cycle - valid_matchesQ[i].baseCycle, valid_matchesQ[i].bw_factor, (2500000000/valid_matchesQ[i].bw_factor)*(second_lowest_cycle - valid_matchesQ[i].baseCycle))
                        #print("Sending " + str((2500000000/valid_matchesQ[i].bw_factor)*(second_lowest_cycle - valid_matchesQ[i].baseCycle)) + " bytes -- ID: " + str(valid_matchesQ[i].id) + " bw_f: " + str(valid_matchesQ[i].bw_factor) + " time: " + str(second_lowest_cycle - valid_matchesQ[i].baseCycle))
                        valid_matchesQ[i].baseCycle = second_lowest_cycle;
                    if valid_matchesQ[i].baseCycle == valid_matchesQ[i].solvedCycle:
                        valid_matchesQ[i].solvedCycle = -1;
                        valid_matchesQ[i].bw_factor = 1;
                # ------------------------------------------------------------------
                #input("")

# *************************************************************************************
#    _  __     _                       
#   | |/ /__ _| |__  _   _ _ __   __ _ 
#   | ' // _` | '_ \| | | | '_ \ / _` |
#   | . \ (_| | | | | |_| | | | | (_| |
#   |_|\_\__,_|_| |_|\__,_|_| |_|\__,_|
# *************************************************************************************
    def __alg__KAHUNA(self, matchQ, col_matchQ, currentPosition):
            
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

            #print("--- VALID MQ ---")
            #for i in range(0, len(valid_matchesQ)):
            #    print(valid_matchesQ[i])
            #print("--- INVALID MQ ---")
            #for i in range(0, len(invalid_matchesQ)):
            #    print(invalid_matchesQ[i])
            #print("----------------")


            while True:
                #print(bcolors.OKBLUE + "********************************************************" + bcolors.ENDC)
                #print(bcolors.OKGREEN + "STEP 1 ---- Found Ready?" + bcolors.ENDC)
                # STEP 1 ---- Found Ready?
                # This step is the stop condition in this never-ending loop
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle == valid_matchesQ[i].endCycle:
                        #print("READY")
                        #print(bcolors.OKCYAN, end='');
                        #print(valid_matchesQ[i])
                        #print(str(valid_matchesQ[i].data_sent) +  " --- " + str(valid_matchesQ[i].size))
                        #print(round(valid_matchesQ[i].data_sent) -  (valid_matchesQ[i].size+16))
                        #print(valid_matchesQ[i])
                        #print(bcolors.ENDC, end='');
                        readyMatch = None;
                        for j in range(0, len(matchQ)):
                            if valid_matchesQ[i].id == matchQ[j].id:
                               readyMatch = matchQ.pop(j)
                               break;
                        if readyMatch is None:
                            for j in range(0, len(col_matchQ)):
                                readyMatch = col_matchQ[j].getMatchByID(valid_matchesQ[i].id);
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
                                   
                        return readyMatch;
                # ------------------------------------------------------------------

                #print(bcolors.OKGREEN + "STEP 2 ---- Find Window" + bcolors.ENDC)
                # STEP 2 ---- Find Window
                # lowest_cycle <----> second_lowest_cycle
                li: int; 
                sli: int;
                lowest_cycle = valid_matchesQ[0].baseCycle;
                li = 0
                for i in range(0, len(valid_matchesQ)):
                    if valid_matchesQ[i].baseCycle < lowest_cycle:
                        lowest_cycle = valid_matchesQ[i].baseCycle;
                        li = i
                
                currentRoundQueue: list[MQ_Match];
                currentRoundQueue = [];
                rankS = valid_matchesQ[li].rankS;
                rankR = valid_matchesQ[li].rankR;
                
                second_lowest_cycle = valid_matchesQ[li].getUpperCycle();
                sli=li;
                for i in range(0, len(valid_matchesQ)):
                    if ( 
                        valid_matchesQ[i].rankS == rankS or
                        valid_matchesQ[i].rankS == rankR or
                        valid_matchesQ[i].rankR == rankS or 
                        valid_matchesQ[i].rankR == rankR
                    ):
                        currentRoundQueue.append(valid_matchesQ[i])
                        if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
                            second_lowest_cycle = valid_matchesQ[i].baseCycle;
                            sli=i;
                        if valid_matchesQ[i].getUpperCycle() < second_lowest_cycle:
                            second_lowest_cycle = valid_matchesQ[i].getUpperCycle();
                            sli=i*-1
                # ------------------------------------------------------------------

                #print(bcolors.OKGREEN + "STEP 3 ---- How Many (share this window)" + bcolors.ENDC)
                # STEP 3 ---- How Many (share this window)
                window_share_count_on_send = 0
                window_share_count_on_recv = 0;
                #window_share_count = 0
                indexes_to_increase_on_sendrecv = []
                indexes_to_increase_on_send = []
                indexes_to_increase_on_recv = []
                for i in range(0, len(currentRoundQueue)):
                    #if currentRoundQueue[i].baseCycle >= lowest_cycle and currentRoundQueue[i].baseCycle < second_lowest_cycle:
                    #    window_share_count = window_share_count + 1;
                    #    indexes_to_increase.append(i)
                    #    continue
                    #if currentRoundQueue[i].endCycle > lowest_cycle and currentRoundQueue[i].endCycle <= second_lowest_cycle:
                    #    window_share_count = window_share_count + 1;
                    #    indexes_to_increase.append(i)
                    #    continue
                    #if currentRoundQueue[i].baseCycle < lowest_cycle and currentRoundQueue[i].endCycle > second_lowest_cycle:
                    #    window_share_count = window_share_count + 1;
                    #    indexes_to_increase.append(i)
                    #    continue

                    if (
                        (currentRoundQueue[i].baseCycle >= lowest_cycle and currentRoundQueue[i].baseCycle < second_lowest_cycle) or
                        (currentRoundQueue[i].endCycle > lowest_cycle and currentRoundQueue[i].endCycle <= second_lowest_cycle) or
                        (currentRoundQueue[i].baseCycle < lowest_cycle and currentRoundQueue[i].endCycle > second_lowest_cycle)
                    ):
                        if (
                            currentRoundQueue[i].rankS == rankS and currentRoundQueue[i].rankR == rankR or
                            currentRoundQueue[i].rankS == rankR and currentRoundQueue[i].rankR == rankS
                        ):
                            indexes_to_increase_on_sendrecv.append(i);
                            continue;
                        if currentRoundQueue[i].rankS == rankS or currentRoundQueue[i].rankR == rankS:
                            indexes_to_increase_on_send.append(i);
                            continue;
                        if currentRoundQueue[i].rankS == rankR or currentRoundQueue[i].rankR == rankR:
                            indexes_to_increase_on_recv.append(i);
                            continue
                        print( bcolors.FAIL, end='')
                        print(str(rankS) + " --- " + str(rankR))
                        print(currentRoundQueue[i])
                        print(bcolors.ENDC, end='')
                        assert False, "This match should not be here"
                # ------------------------------------------------------------------

                #print(bcolors.OKGREEN + "STEP 4 ---- Increase" + bcolors.ENDC)
                # STEP 4 ---- Increase
                window_size = second_lowest_cycle - lowest_cycle;
                #newFactor = window_share_count;
                newFactor_on_send = len(indexes_to_increase_on_send) + len(indexes_to_increase_on_sendrecv)
                newFactor_on_recv = len(indexes_to_increase_on_recv) + len(indexes_to_increase_on_sendrecv)
                newFactor_on_sendrecv = (newFactor_on_send) if (newFactor_on_send > newFactor_on_recv) else (newFactor_on_recv);
                
                #print("><><><><><><><><><><><><><><")
                #print(newFactor_on_send)
                #print(newFactor_on_recv)
                #print(newFactor_on_sendrecv)
                #print("><><><><><><><><><><><><><><")


                increment_list_on_send = []
                increment_list_on_recv = []
                increment_list_on_sendrecv = []

                for i in range(0, len(indexes_to_increase_on_send)):
                    curIndex = indexes_to_increase_on_send[i];
                    currentFactor = currentRoundQueue[curIndex].bw_factor;
                    newFactor = newFactor_on_send;
                    increment = (window_size * (newFactor / currentFactor) ) - window_size;
                    increment_list_on_send.append([curIndex, increment]);
                    assert increment >= 0, "increment can not be negative"

                for i in range(0, len(indexes_to_increase_on_recv)):
                    curIndex = indexes_to_increase_on_recv[i];
                    currentFactor = currentRoundQueue[curIndex].bw_factor;
                    newFactor = newFactor_on_recv;
                    increment = (window_size * (newFactor / currentFactor) ) - window_size;
                    increment_list_on_recv.append([curIndex, increment]);
                    assert increment >= 0, "increment can not be negative"

                for i in range(0, len(indexes_to_increase_on_sendrecv)):
                    curIndex = indexes_to_increase_on_sendrecv[i];
                    currentFactor = currentRoundQueue[curIndex].bw_factor;
                    newFactor = newFactor_on_sendrecv;
                    #print(str(currentFactor) + " --> " + str(newFactor))
                    increment = (window_size * (newFactor / currentFactor) ) - window_size;
                    increment_list_on_sendrecv.append([curIndex, increment]);
                    assert increment >= 0, "increment can not be negative"
                
                smallest_increment = increment_list_on_sendrecv[0][1]
                for i in range(0, len(increment_list_on_send)):
                    if increment_list_on_send[i][1] < smallest_increment:
                        smallest_increment = increment_list_on_send[i][1];
                for i in range(0, len(increment_list_on_recv)):
                    if increment_list_on_recv[i][1] < smallest_increment:
                        smallest_increment = increment_list_on_recv[i][1];
                for i in range(0, len(increment_list_on_sendrecv)):
                    if increment_list_on_sendrecv[i][1] < smallest_increment:
                        smallest_increment = increment_list_on_sendrecv[i][1];
                


                
                #for i in range(0, len(indexes_to_increase)):
                #    curIndex = indexes_to_increase[i];
                #    currentFactor = currentRoundQueue[curIndex].bw_factor;
                #    increment = (window_size * (newFactor / currentFactor) ) - window_size;
                #    assert increment >= 0, "increment can not be negative"
                #    increment_list.append(increment);
                #smallest_increment = increment_list[0];
                #for i in range(1, len(increment_list)):
                #    if increment_list[i] < smallest_increment:
                #        smallest_increment = increment_list[i];

                for i in range(0, len(increment_list_on_send)):
                    curIndex = increment_list_on_send[i][0];
                    increment = increment_list_on_send[i][1]
                    flooded_increment = 0;
                    if increment > smallest_increment:
                        flooded_increment = (increment - smallest_increment) * 1 / newFactor_on_send;
                        assert flooded_increment >= 0, "flooded increment can not be negative"
                    assert currentRoundQueue[curIndex].solvedCycle == -1, "solvedCycle need to be -1 here"
                    currentRoundQueue[curIndex].endCycle = currentRoundQueue[curIndex].endCycle + smallest_increment + flooded_increment;
                    currentRoundQueue[curIndex].solvedCycle = currentRoundQueue[curIndex].baseCycle + window_size + smallest_increment;
                    currentRoundQueue[curIndex].bw_factor = newFactor_on_send;

                for i in range(0, len(increment_list_on_recv)):
                    curIndex = increment_list_on_recv[i][0];
                    increment = increment_list_on_recv[i][1]
                    flooded_increment = 0;
                    if increment > smallest_increment:
                        flooded_increment = (increment - smallest_increment) * 1 / newFactor_on_recv;
                        assert flooded_increment >= 0, "flooded increment can not be negative"
                    assert currentRoundQueue[curIndex].solvedCycle == -1, "solvedCycle need to be -1 here"
                    currentRoundQueue[curIndex].endCycle = currentRoundQueue[curIndex].endCycle + smallest_increment + flooded_increment;
                    currentRoundQueue[curIndex].solvedCycle = currentRoundQueue[curIndex].baseCycle + window_size + smallest_increment;
                    currentRoundQueue[curIndex].bw_factor = newFactor_on_recv;

                for i in range(0, len(increment_list_on_sendrecv)):
                    curIndex = increment_list_on_sendrecv[i][0];
                    increment = increment_list_on_sendrecv[i][1]
                    flooded_increment = 0;
                    if increment > smallest_increment:
                        flooded_increment = (increment - smallest_increment) * 1 / newFactor_on_sendrecv;
                        assert flooded_increment >= 0, "flooded increment can not be negative"
                    assert currentRoundQueue[curIndex].solvedCycle == -1, "solvedCycle need to be -1 here"
                    currentRoundQueue[curIndex].endCycle = currentRoundQueue[curIndex].endCycle + smallest_increment + flooded_increment;
                    currentRoundQueue[curIndex].solvedCycle = currentRoundQueue[curIndex].baseCycle + window_size + smallest_increment;
                    currentRoundQueue[curIndex].bw_factor = newFactor_on_sendrecv;

                #for i in range(0, len(indexes_to_increase)):
                #    curIndex = indexes_to_increase[i];
                #    increment = increment_list[i];
                #    flooded_increment = 0;
                #    if increment > smallest_increment:
                #        flooded_increment = (increment - smallest_increment) * 1 / newFactor;
                #        assert flooded_increment >= 0, "flooded increment can not be negative"
                #    assert currentRoundQueue[curIndex].solvedCycle == -1, "solvedCycle need to be -1 here"
                #    #if currentRoundQueue[curIndex].solvedCycle != -1:
                #    #    decrement = currentRoundQueue[curIndex].solvedCycle - (currentRoundQueue[curIndex].baseCycle + window_size)
                #    #    decrement = decrement - decrement * 1/currentRoundQueue[curIndex].bw_factor
                #    #    currentRoundQueue[curIndex].endCycle = currentRoundQueue[curIndex].endCycle - decrement
                #    currentRoundQueue[curIndex].endCycle = currentRoundQueue[curIndex].endCycle + smallest_increment + flooded_increment;
                #    currentRoundQueue[curIndex].solvedCycle = currentRoundQueue[curIndex].baseCycle + window_size + smallest_increment;
                #    currentRoundQueue[curIndex].bw_factor = newFactor;
                # ------------------------------------------------------------------

                #print(bcolors.OKGREEN + "STEP 5 ---- Crop" + bcolors.ENDC)
                # STEP 5 ---- Crop
                lci = currentRoundQueue[indexes_to_increase_on_sendrecv[0]].id;
                lowest_cycle = currentRoundQueue[indexes_to_increase_on_sendrecv[0]].baseCycle;
                slci = lci
                second_lowest_cycle = currentRoundQueue[indexes_to_increase_on_sendrecv[0]].getUpperCycle();                
                for i in range(0, len(currentRoundQueue)):
                    if currentRoundQueue[i].baseCycle < second_lowest_cycle and currentRoundQueue[i].baseCycle != lowest_cycle:
                        second_lowest_cycle = currentRoundQueue[i].baseCycle;
                        slci = currentRoundQueue[i].id;
                    if currentRoundQueue[i].getUpperCycle() < second_lowest_cycle:
                        assert False, "Mah que que eh isso!?" # Is it possible to get here?
                        second_lowest_cycle = currentRoundQueue[i].getUpperCycle();

                for i in range(0, len(currentRoundQueue)):
                    if currentRoundQueue[i].baseCycle < second_lowest_cycle:
                        #valid_matchesQ[i].transmitted_data.append([second_lowest_cycle - valid_matchesQ[i].baseCycle, valid_matchesQ[i].bw_factor, (2500000000/valid_matchesQ[i].bw_factor)*(second_lowest_cycle - valid_matchesQ[i].baseCycle) ])
                        currentRoundQueue[i].includeTransmittedData(second_lowest_cycle - currentRoundQueue[i].baseCycle, currentRoundQueue[i].bw_factor, (2500000000/currentRoundQueue[i].bw_factor)*(second_lowest_cycle - currentRoundQueue[i].baseCycle))
                        #print("Sending " + str((2500000000/valid_matchesQ[i].bw_factor)*(second_lowest_cycle - valid_matchesQ[i].baseCycle)) + " bytes -- ID: " + str(valid_matchesQ[i].id) + " bw_f: " + str(valid_matchesQ[i].bw_factor) + " time: " + str(second_lowest_cycle - valid_matchesQ[i].baseCycle))
                        currentRoundQueue[i].baseCycle = second_lowest_cycle;
                    if currentRoundQueue[i].baseCycle == currentRoundQueue[i].solvedCycle:
                        currentRoundQueue[i].solvedCycle = -1;
                        currentRoundQueue[i].bw_factor = 1;
                    if currentRoundQueue[i].solvedCycle != -1:
                        decrement = currentRoundQueue[i].solvedCycle - (currentRoundQueue[i].baseCycle)
                        decrement = decrement - decrement * 1/currentRoundQueue[i].bw_factor
                        currentRoundQueue[i].endCycle = currentRoundQueue[i].endCycle - decrement
                        currentRoundQueue[i].solvedCycle = -1;
                        currentRoundQueue[i].bw_factor = 1;
                # ------------------------------------------------------------------
                #input("")


    # matchQ: matches queue
    # col_matchQ: Collective matches queue
    # currentPosition: vector with position of SEND/RECV for each rank
    def processContention(self, matchQ, col_matchQ, currentPosition):


        if (self.topology == "SC_SHARED"):
            return self.__alg_SC_SHARED(matchQ, col_matchQ, currentPosition);
        
        if (self.topology == "KAHUNA"):
            return self.__alg__KAHUNA(matchQ, col_matchQ, currentPosition);

        print( bcolors.FAIL + "ERROR: Unknown topology " + self.topology + bcolors.ENDC);
        sys.exit(1);

        assert False, "We are analyzing SC_SHARED, WTF are you doing here?"

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


            
            
        print( bcolors.FAIL + "ERROR: Unknown topology " + self.topology + bcolors.ENDC);
        sys.exit(1);

