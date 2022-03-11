from Topology import *





class FMU_CircularBuffer:

    class CircularBufferInput:

        def __init__(self, match_id: int, size: int):
            self.match_id = match_id;
            self.size = size;
            self.valid = True;

        def invalidate(self):
            assert self.valid;
            self.valid = False;


    def __init__(self, nFMUs: int):
        assert nFMUs > 0;
        self.nFMUs = nFMUs;
        self.circular_buffer = []
        for i in range(0, nFMUs):
            self.circular_buffer.append([])
        self.biggest_buffer_size = 0;

    def insert_entry(self, fmu: int, match_id: int, size: int) -> None:
        assert fmu < self.nFMUs
        newInput = self.CircularBufferInput(match_id, size);
        self.circular_buffer[fmu].append(newInput);


    def get_total_size_on_fmu(self, fmu: int) -> int:
        assert fmu < self.nFMUs
        total_size = 0;
        fmu_buffer: typing.List[self.CircularBufferInput] = self.circular_buffer[fmu];
        for i in range(0, len(fmu_buffer)):
            total_size = total_size + fmu_buffer[i].size;
        if total_size > self.biggest_buffer_size:
            self.biggest_buffer_size = total_size;
        return total_size;

            

    def consume_buffer(self, fmu: int) -> None:
        self.get_total_size_on_fmu(fmu); # We decided to put this here, to calculate the biggest size every time the buffer is consumed.
                                         # To consume the buffer is to move forward the head of the buffer.
        fmu_buffer = self.circular_buffer[fmu];
        assert len(fmu_buffer) > 0;
        while fmu_buffer:
            if not fmu_buffer[0].valid:
                del fmu_buffer[0];
            else:
                break;


    def consume_entry(self, fmu: int, match_id: int):
        fmu_buffer : typing.List[self.CircularBufferInput];
        fmu_buffer = self.circular_buffer[fmu];
        for i in range(0, len(fmu_buffer)):
            if fmu_buffer[i].match_id == match_id:
                fmu_buffer[i].invalidate();
                self.consume_buffer(fmu); # Always try to consume the buffer when an entry is invalidated (consumed)
                return None;
        assert False, "Could not find a match with given ID on given FMU"



class TopFreeMemoryIndependent(Topology):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopFreeMemoryIndependent, self).__init__(nRanks, configfile);
        self.nFMUs = configfile.number_of_FMUs;
        self.independent_send_recv = True;
        assert self.nFMUs > 0, "Number of Free Memory Units needs to be at least 1 when using FMUs topology"
        #assert self.eager_protocol_max_size == 0, "Eager Protocol can not be activated with this Topology"

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = FMU_CircularBuffer(self.nFMUs);




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

        # *******************************************************************************************************************************


        # Check for not initialized matches, and initialize them
        for i in range(len(valid_matchesQ)):
            if not valid_matchesQ[i].initialized:
                valid_matchesQ[i].sep_initializeMatch(self.SimpleCommunicationCalculusInternode(valid_matchesQ[i].size));


        # find lowest cycle
        readyMatch : MQ_Match
        readyMatch = None;

        while readyMatch == None:

            lowest_cycle = valid_matchesQ[0].sep_getBaseCycle();
            li = 0
            for i in range(0, len(valid_matchesQ)):
                if valid_matchesQ[i].sep_getBaseCycle() < lowest_cycle:
                    lowest_cycle = valid_matchesQ[i].sep_getBaseCycle();
                    li = i

            readyMatch = valid_matchesQ[li];

            rank_in_usage = None;
            if readyMatch.still_solving_send:
                rank_in_usage = readyMatch.rankS;
            else:
                rank_in_usage = readyMatch.rankR;
            
            # Delay other communications as needed
            for j in range(0, len(valid_matchesQ)):
                if j == li:
                    continue;
                
                
                partner_rank = None;
                if valid_matchesQ[j].still_solving_send:
                    partner_rank = valid_matchesQ[j].rankS;
                else:
                    partner_rank = valid_matchesQ[j].rankR;

                if (
                               (rank_in_usage == partner_rank) or
                               (readyMatch.rankR % self.nFMUs == valid_matchesQ[j].rankR % self.nFMUs)
                ):
                    minToStart = readyMatch.sep_getEndCycle() + valid_matchesQ[j].latency;
                    inc = minToStart - valid_matchesQ[j].sep_getBaseCycle();

                    if inc > 0:
                        valid_matchesQ[j].sep_incrementCycle(inc);

            for j in range(0, len(invalid_matchesQ)):
                
                partner_rank = None;
                if invalid_matchesQ[j].still_solving_send:
                    partner_rank = invalid_matchesQ[j].rankS;
                else:
                    partner_rank = invalid_matchesQ[j].rankR;
                
                
                if (
                               (rank_in_usage == partner_rank) or
                               (readyMatch.rankR % self.nFMUs == invalid_matchesQ[j].rankR % self.nFMUs)
                ):
                    minToStart = readyMatch.sep_getEndCycle() + invalid_matchesQ[j].latency;
                    inc = minToStart - invalid_matchesQ[j].sep_getBaseCycle();

                    if inc > 0:
                        invalid_matchesQ[j].sep_incrementCycle(inc);


            if readyMatch.still_solving_send:
                readyMatch.sep_move_RECV_after_SEND();
                
                self.fmu_circularBuffer.insert_entry(readyMatch.rankR % self.nFMUs,
                                                     readyMatch.id,
                                                     readyMatch.size);
                
                readyMatch = None;



        
        self.fmu_circularBuffer.consume_entry(readyMatch.rankR % self.nFMUs,
                                              readyMatch.id);
        

        readyMatchID = readyMatch.id;
        # Grab the ready match from the matches queue (matchQ) or collectives matches queue (col_matchQ)
        readyMatch = None;
        for j in range(0, len(matchQ)):
            if readyMatchID == matchQ[j].id:
               readyMatch = matchQ.pop(j)
               break;
        if readyMatch is None:
            for j in range(0, len(col_matchQ)):
                readyMatch = col_matchQ[j].getMatchByID(readyMatchID);
                if readyMatch is not None:
                    break;
        # If readyMatch is None, it does not exist... what happened?        
        assert readyMatch is not None, "ready match is not presented on matches queues"


        #print("Processing contention complete.")
        return readyMatch;


        
        