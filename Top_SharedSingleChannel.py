from Topology import *


class TopSharedSingleChannel(Topology):

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
                        #print(valid_matchesQ[i].data_sent)
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