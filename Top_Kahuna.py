from Topology import *




class TopKahuna(Topology):



    def findReadyMatch(self, valid_matchesQ: typing.List[MQ_Match]) -> MQ_Match:
        for i in range(0, len(valid_matchesQ)):
            if valid_matchesQ[i].baseCycle == valid_matchesQ[i].endCycle:
                return valid_matchesQ[i];
        return None;


    def findWindow(self, valid_matchesQ: typing.List[MQ_Match]) -> typing.Tuple[float, float]:
        assert len(valid_matchesQ) > 0
        lowest_cycle = valid_matchesQ[0].baseCycle;
        for i in range(0, len(valid_matchesQ)):
                        if valid_matchesQ[i].baseCycle < lowest_cycle:
                            lowest_cycle = valid_matchesQ[i].baseCycle;

        second_lowest_cycle = valid_matchesQ[0].getUpperCycle
        assert lowest_cycle < second_lowest_cycle, "second lowest not bigger than lowest?"

        for i in range(0, len(valid_matchesQ)):
        
            if valid_matchesQ[i].baseCycle < second_lowest_cycle and valid_matchesQ[i].baseCycle != lowest_cycle:
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

        return None;

    def cropPhase(
        self,
        valid_matchesQ: typing.List[MQ_Match],
        ):
        return None;


    def processContention(self, matchQ, col_matchQ, currentPosition)-> MQ_Match:
        
        valid_matchesQ : list[MQ_Match]; # For valid matches
        valid_matchesQ = []
        invalid_matchesQ : list[MQ_Match]; # For invalid matches
        invalid_matchesQ = []

        valid_matchesQ, invalid_matchesQ = self.separateValidAndInvalidMatches(matchQ, col_matchQ, currentPosition);

        times: int;
        times = 0;

        while True:

            # Step 1 ---- Found Ready?
            readyMatch: MQ_Match;
            readyMatch = self.findReadyMatch(valid_matchesQ);

            if readyMatch is not None:
                # TODO implement this part!
                pass
            # ---

            # Step 2 ---- Find Window
            lowest_cycle: float
            second_lowest_cycle: float

            lowest_cycle, second_lowest_cycle = self.findWindow(valid_matchesQ);
            # ---

            # Step 3 and 4 ---- How Many? Increase!
            self.increaseMatchesDueSharing(valid_matchesQ, lowest_cycle, second_lowest_cycle)
            # ---
            
            # Crop
            self.cropPhase(valid_matchesQ);
            # ---

            times = times + 1; # Debug

        
        return None;