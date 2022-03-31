from MPI_Constants import *


class Contention_Kahuna:

    @staticmethod
    def findReadyMatch(valid_matchesQ: typing.List[MQ_Match]) -> MQ_Match:
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle == valid_matchesQ[i].endCycle:
                return valid_matchesQ[i];
        return None;

    @staticmethod
    def findWindow(valid_matchesQ: typing.List[MQ_Match]) -> typing.Tuple[float, float]:
        assert len(valid_matchesQ) > 0
        lowest_cycle = valid_matchesQ[0].baseCycle;
        for i in range(0, len(valid_matchesQ)):
                        if valid_matchesQ[i].baseCycle < lowest_cycle:
                            lowest_cycle = valid_matchesQ[i].baseCycle;

        second_lowest_cycle = valid_matchesQ[0].getUpperCycle()
        assert lowest_cycle < second_lowest_cycle, "second lowest not bigger than lowest?"

        for i in range(0, len(valid_matchesQ)):
        
            if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
                second_lowest_cycle = valid_matchesQ[i].baseCycle;
            if valid_matchesQ[i].getUpperCycle() < second_lowest_cycle:
                second_lowest_cycle = valid_matchesQ[i].getUpperCycle();

        return lowest_cycle, second_lowest_cycle;


    @staticmethod
    def increaseMatchesDueSharing(
        nRanks: int,
        valid_matchesQ: typing.List[MQ_Match],
        lowest_cycle: float,
        second_lowest_cycle: float,
        ):


        # How Many? (using sharingVector)
        sharingVector: list[int];
        sharingVector = [0] * nRanks;
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

    @staticmethod
    def cropPhase(
        valid_matchesQ: typing.List[MQ_Match],
        lowest_cycle
        ):

        second_lowest_cycle = valid_matchesQ[0].getUpperCycle();
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
                second_lowest_cycle = valid_matchesQ[i].baseCycle;
            if valid_matchesQ[i].getUpperCycle() < second_lowest_cycle:
                #assert False, "Is it possible to get here!?" # Is it possible to get here?
                second_lowest_cycle = valid_matchesQ[i].getUpperCycle();

        # This loop is the Crop
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle < second_lowest_cycle:
                cropping_window = second_lowest_cycle - valid_matchesQ[i].baseCycle;
                current_bw = valid_matchesQ[i].bw_factor;
                sent_data = cropping_window * valid_matchesQ[i].bw / current_bw;
                valid_matchesQ[i].includeTransmittedData(cropping_window, current_bw, sent_data);
                valid_matchesQ[i].baseCycle = second_lowest_cycle;

                if valid_matchesQ[i].baseCycle == valid_matchesQ[i].solvedCycle:
                    valid_matchesQ[i].bw_factor = 1;
                    valid_matchesQ[i].solvedCycle = -1;

        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].bw_factor != 1:
                solved_window = valid_matchesQ[i].solvedCycle - valid_matchesQ[i].baseCycle;
                decrement = solved_window * (1.0/float(valid_matchesQ[i].bw_factor))
                decrement = solved_window - decrement
                valid_matchesQ[i].endCycle = valid_matchesQ[i].endCycle - decrement;
                assert valid_matchesQ[i].endCycle > valid_matchesQ[i].baseCycle;
                valid_matchesQ[i].solvedCycle = -1;
                valid_matchesQ[i].bw_factor = 1;

            # GAMBIARRA: Should investigate why we need to do this. 
            # Sometimes the result does not reach the endCycle, and we do not know why this happens.
            if ((valid_matchesQ[i].baseCycle - valid_matchesQ[i].original_baseCycle)/(valid_matchesQ[i].endCycle - valid_matchesQ[i].original_baseCycle)) > 0.998:
                        valid_matchesQ[i].baseCycle = valid_matchesQ[i].endCycle;

        

        return None;





