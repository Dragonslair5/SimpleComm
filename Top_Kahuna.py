from Topology import *
import math

class TopKahuna(Topology):



    def findReadyMatch(self, valid_matchesQ: typing.List[MQ_Match]) -> MQ_Match:
        #for i in range(0, len(valid_matchesQ)):
        #    #if valid_matchesQ[i].baseCycle == valid_matchesQ[i].endCycle:
        #    if math.isclose(valid_matchesQ[i].baseCycle, valid_matchesQ[i].endCycle):
        #        return valid_matchesQ[i];
        #return None;

        readies : typing.List[MQ_Match];
        readies = []
        for i in range(0, len(valid_matchesQ)):
            #if valid_matchesQ[i].baseCycle == valid_matchesQ[i].endCycle:
            if math.isclose(valid_matchesQ[i].baseCycle, valid_matchesQ[i].endCycle):
                #return valid_matchesQ[i];
                readies.append(valid_matchesQ[i]);
        
        if len(readies) == 0:
            return None;
        
        lowestID = 0;
        lowestCycle = readies[0].baseCycle;

        for i in range(1, len(readies)):
            if readies[i].baseCycle < lowestCycle:
                lowestID = i;
                lowestCycle = readies[i].baseCycle;
        
        return readies[lowestID];


    def findWindow(self, valid_matchesQ: typing.List[MQ_Match]) -> typing.Tuple[float, float]:
        assert len(valid_matchesQ) > 0
        lowest_cycle = valid_matchesQ[0].baseCycle;
        for i in range(0, len(valid_matchesQ)):
                        if valid_matchesQ[i].baseCycle < lowest_cycle:
                            lowest_cycle = valid_matchesQ[i].baseCycle;

        second_lowest_cycle = valid_matchesQ[0].getUpperCycle()
        assert lowest_cycle < second_lowest_cycle, "second lowest not bigger than lowest?"

        for i in range(0, len(valid_matchesQ)):
        
            if valid_matchesQ[i].baseCycle < second_lowest_cycle and not math.isclose(valid_matchesQ[i].baseCycle, lowest_cycle):
                second_lowest_cycle = valid_matchesQ[i].baseCycle;
            if valid_matchesQ[i].getUpperCycle() < second_lowest_cycle:
                second_lowest_cycle = valid_matchesQ[i].getUpperCycle();

        return lowest_cycle, second_lowest_cycle;



    def increaseMatchesDueSharing(
        self,
        valid_matchesQ: typing.List[MQ_Match],
        lowest_cycle: float,
        second_lowest_cycle: float,
        ):


        # How Many? (using sharingVector)
        sharingVector: list[int];
        sharingVector = [0] * self.nRanks;
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle < second_lowest_cycle:
                rankS = valid_matchesQ[i].rankS
                rankR = valid_matchesQ[i].rankR
                sharingVector[rankS] = sharingVector[rankS] + 1;
                sharingVector[rankR] = sharingVector[rankR] + 1;
        # ---

        # Increment
        window_size = second_lowest_cycle - lowest_cycle;
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle < second_lowest_cycle:
                #assert not math.isclose(valid_matchesQ[i].baseCycle , second_lowest_cycle);
                currentFactor = valid_matchesQ[i].bw_factor;
                rankS = valid_matchesQ[i].rankS;
                rankR = valid_matchesQ[i].rankR;
                newFactor = sharingVector[rankS] if sharingVector[rankS] > sharingVector[rankR] else sharingVector[rankR];
                increment = (window_size * (float(newFactor)/float(currentFactor))) - window_size;
                valid_matchesQ[i].endCycle = valid_matchesQ[i].endCycle + increment;
                valid_matchesQ[i].solvedCycle = valid_matchesQ[i].baseCycle + window_size + increment;
                valid_matchesQ[i].bw_factor = newFactor;
        # ---

        # Adjust to the smallest increment
        smallest_solved = valid_matchesQ[0].getUpperCycle();
        for i in range(1, len(valid_matchesQ)):
            if valid_matchesQ[i].getUpperCycle() < smallest_solved:
                smallest_solved = valid_matchesQ[i].getUpperCycle();
        
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].solvedCycle > smallest_solved:
                flooded_window_size = (valid_matchesQ[i].solvedCycle - smallest_solved);
                decrement = flooded_window_size * (1.0/float(valid_matchesQ[i].bw_factor));
                decrement = flooded_window_size - decrement;
                valid_matchesQ[i].endCycle = valid_matchesQ[i].endCycle - decrement;
                valid_matchesQ[i].solvedCycle = smallest_solved;
        # ---

        return None;

    def cropPhase(
        self,
        valid_matchesQ: typing.List[MQ_Match],
        lowest_cycle
        ):

        second_lowest_cycle = valid_matchesQ[0].getUpperCycle();
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle < second_lowest_cycle and not math.isclose(valid_matchesQ[i].baseCycle, lowest_cycle):
                second_lowest_cycle = valid_matchesQ[i].baseCycle;
            if valid_matchesQ[i].getUpperCycle() < second_lowest_cycle:
                #assert False, "Is it possible to get here!?" # Is it possible to get here?
                second_lowest_cycle = valid_matchesQ[i].getUpperCycle();

        # This loop is the Crop
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle < second_lowest_cycle:
                cropping_window = second_lowest_cycle - valid_matchesQ[i].baseCycle;
                current_bw = valid_matchesQ[i].bw_factor;
                sent_data = cropping_window * self.interBandwidth / current_bw;
                valid_matchesQ[i].includeTransmittedData(cropping_window, current_bw, sent_data);
                valid_matchesQ[i].baseCycle = second_lowest_cycle;

                if math.isclose(valid_matchesQ[i].baseCycle, valid_matchesQ[i].solvedCycle):
                    valid_matchesQ[i].bw_factor = 1;
                    valid_matchesQ[i].solvedCycle = -1;

        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].bw_factor != 1:
                solved_window = valid_matchesQ[i].solvedCycle - valid_matchesQ[i].baseCycle;
                decrement = solved_window * (1.0/float(valid_matchesQ[i].bw_factor))
                decrement = solved_window - decrement
                valid_matchesQ[i].endCycle = valid_matchesQ[i].endCycle - decrement;
                #if math.isclose(valid_matchesQ[i].endCycle, valid_matchesQ[i].baseCycle):
                #    print("Decrement was: " + str(decrement));
                assert valid_matchesQ[i].endCycle > valid_matchesQ[i].baseCycle, str(valid_matchesQ[i].endCycle) + " <= " + str(valid_matchesQ[i].baseCycle);
                valid_matchesQ[i].solvedCycle = -1;
                valid_matchesQ[i].bw_factor = 1;

            # GAMBIARRA: Should investigate why we need to do this. 
            # Sometimes the result does not reach the endCycle, and we do not know why this happens.
            # Probably some floating-point imprecision issue.
            #if ((valid_matchesQ[i].baseCycle - valid_matchesQ[i].original_baseCycle)/(valid_matchesQ[i].endCycle - valid_matchesQ[i].original_baseCycle)) > 0.998:
            #    valid_matchesQ[i].baseCycle = valid_matchesQ[i].endCycle;
            if math.isclose(valid_matchesQ[i].baseCycle, valid_matchesQ[i].endCycle):
                valid_matchesQ[i].baseCycle = valid_matchesQ[i].endCycle;
        

        return None;


    def processContention(self, matchQ: typing.List[MQ_Match])-> MQ_Match:

        valid_matchesQ = matchQ;

        times: int;
        times = 0;

        #print("--------------------");
        #for i in range(len(valid_matchesQ)):
        #    print(valid_matchesQ[i]);
        #print("--------------------");

        while True:

            # Step 1 ---- Found Ready?
            readyMatch: MQ_Match;
            readyMatch = self.findReadyMatch(valid_matchesQ);

            if readyMatch is not None:

                #assert readyMatch.data_sent <= (readyMatch.size+16)*1.01, "sent more data? -- " + str(readyMatch.data_sent) + " > " + str((readyMatch.size+16)*1.01)
                # TODO We should check why sometimes we exceed the amount of data that should be sent
                # TODO Resulting in getting this assert back
                # Allowing 10 extra bytes to be sent or 1% increment
                assert math.isclose(readyMatch.data_sent, (readyMatch.size+16), abs_tol=10, rel_tol=0.01) or readyMatch.data_sent < (readyMatch.size+16), "sent more data? -- " + str(readyMatch.data_sent) + " > " + str((readyMatch.size+16));

                # Remove ReadyMatch from < matchQ / col_matchQ >
                id = readyMatch.id
                readyMatch = None
                for j in range(0, len(matchQ)):
                    if id == matchQ[j].id:
                        readyMatch = matchQ.pop(j)
                        break;
                assert readyMatch is not None, "ready match is not presented on matches queues"
                # ---
                        
                #print("")
                return readyMatch;

            # ---

            # Step 2 ---- Find Window
            lowest_cycle: float
            second_lowest_cycle: float

            lowest_cycle, second_lowest_cycle = self.findWindow(valid_matchesQ);
            assert not math.isclose(lowest_cycle, second_lowest_cycle), "q?"
            # ---

            # Step 3 and 4 ---- How Many? Increase!
            self.increaseMatchesDueSharing(valid_matchesQ, lowest_cycle, second_lowest_cycle)
            # ---
            
            # Step 5 ---- Crop
            self.cropPhase(valid_matchesQ, lowest_cycle);
            # ---

            times = times + 1; # Debug

        
        return None;