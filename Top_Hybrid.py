from Topology import *
from FMU_CircularBuffer import *
from Contention_Kahuna import *




# The initial intent of this Topology is to be a topology of topologies.
class TopHybrid(Topology):


    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopHybrid, self).__init__(nRanks, configfile);

        self.nFMUs = configfile.number_of_FMUs;
        self.independent_send_recv = True;
        assert self.nFMUs > 0, "Number of Free Memory Units needs to be at least 1 when using FMUs topology"

        self.pivotValue = configfile.fmu_pivot_value;
        self.fmu_bandwidth = configfile.fmu_bandwidth;
        self.fmu_latency = configfile.fmu_latency;

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = FMU_CircularBuffer(self.nFMUs);


    
    # Decide if a match should be served by FMU
    def isThroughFMU(self, rankS: int, rankR: int, size: int)->bool:

        if size >= self.pivotValue:
            return True;
        return False


    # Override
    def CommunicationCalculus_Bandwidth(self, rankS: int, rankR: int, workload: int):

        workload = int(workload) + 16; # 16 Bytes as MPI overhead (based on SimGrid)

        nodeS = rankS // self.cores_per_node;
        nodeR = rankR // self.cores_per_node;

        bandwidth: float;
        if nodeS == nodeR: # Intranode
            bandwidth=self.intraBandwidth;
        else: #Internode
            if self.isThroughFMU(rankS, rankR, workload):
                bandwidth=self.fmu_bandwidth;
            else:
                bandwidth=self.interBandwidth;

        if bandwidth == 0:
            return 0, bandwidth;
        else:
            return workload/bandwidth, bandwidth;






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
        # We separate the matches on two arrays, one for the valid ones (valid_matchesQ)
        # and another for the invalid ones (invalid_matchesQ)
        valid_matchesQ, invalid_matchesQ = self.separateValidAndInvalidMatches(matchQ, col_matchQ, currentPosition);
        # We might be on a deadlock if there is no valid match on this point
        assert len(valid_matchesQ) > 0, "No valid Match was found"
        # *******************************************************************************************************************************


        network_matchesQ: list[MQ_Match];
        network_matchesQ = [];

        fmu_matchesQ: list[MQ_Match];
        fmu_matchesQ = [];

        for i in range(len(valid_matchesQ)):
            match = valid_matchesQ[i];
            if self.isThroughFMU(match.rankS, match.rankR, match.size):
                fmu_matchesQ.append(match);
            else:
                network_matchesQ.append(match);

        print(str(len(valid_matchesQ)) + " " + str(len(network_matchesQ)) + " " + str(len(fmu_matchesQ)))

        # Now we have
            #invalid_matchesQ
            #network_matchesQ
            #fmu_matchesQ


        # Initialize matches on FMU
        for i in range(len(fmu_matchesQ)):
            if not fmu_matchesQ[i].initialized:
                fmu_matchesQ[i].sep_initializeMatch(self.CommunicationCalculus_Bandwidth(fmu_matchesQ[i].rankS, fmu_matchesQ[i].rankR, fmu_matchesQ[i].size)[0])
        
    

        lowest_cycle_fmu: float;
        lowest_cycle_fmu = None;
        lowest_cycle_network: float;
        lowest_cycle_network = None;

        if len(fmu_matchesQ) > 0:
            lowest_cycle_fmu = fmu_matchesQ[0].sep_getBaseCycle();

        if len(network_matchesQ) > 0:
            lowest_cycle_network = Contention_Kahuna.findWindow(network_matchesQ);

        readyMatchID: int = None;


        assert len(fmu_matchesQ) == 0;
        readyMatchID = self.Contention_Kahuna(network_matchesQ, invalid_matchesQ, -1);

        '''
        if lowest_cycle_fmu == None:
            readyMatchID = self.Contention_Kahuna(network_matchesQ, invalid_matchesQ, -1);
        else:
            if lowest_cycle_network == None:
                readyMatchID = self.Contention_FMU(fmu_matchesQ, invalid_matchesQ);
            else:
                if lowest_cycle_fmu <= lowest_cycle_network:
                    readyMatchID = self.Contention_FMU(fmu_matchesQ, invalid_matchesQ);
                else:
                    readyMatchID = self.Contention_Kahuna(network_matchesQ, invalid_matchesQ, lowest_cycle_fmu);
                    if readyMatchID == None:
                        readyMatchID = self.Contention_FMU(fmu_matchesQ, invalid_matchesQ);
        '''

        print("*****" + str(readyMatchID) + "**********")
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

        return readyMatch;





    def Contention_Kahuna(self, valid_matchesQ: typing.List[MQ_Match], invalid_matchesQ: typing.List[MQ_Match], time_limit: float)-> int:


        times: int;
        times = 0;

        while True:

            # Step 1 ---- Found Ready?
            readyMatch: MQ_Match;
            readyMatch = Contention_Kahuna.findReadyMatch(valid_matchesQ);

            if readyMatch is not None:

                assert readyMatch.data_sent <= readyMatch.size+16, "sent more data?"
                
                '''
                # Remove ReadyMatch from < matchQ / col_matchQ >
                id = readyMatch.id
                readyMatch = None
                for j in range(0, len(matchQ)):
                    if id == matchQ[j].id:
                        readyMatch = matchQ.pop(j)
                        break;
                if readyMatch is None:
                    for j in range(0, len(col_matchQ)):
                        readyMatch = col_matchQ[j].getMatchByID(id);
                        if readyMatch is not None:
                            break;
                # If readyMatch is None, it does not exist... what happened?        
                assert readyMatch is not None, "ready match is not presented on matches queues"
                # ---
                '''

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
                        
                #print("")
                #return readyMatch;
                return readyMatch.id;

            # ---

            # Step 2 ---- Find Window
            lowest_cycle: float
            second_lowest_cycle: float

            lowest_cycle, second_lowest_cycle = Contention_Kahuna.findWindow(valid_matchesQ);
            
            #if lowest_cycle >= time_limit and time_limit > 0:
            #    return None;
                        
            # ---

            # Step 3 and 4 ---- How Many? Increase!
            Contention_Kahuna.increaseMatchesDueSharing(self.nRanks, valid_matchesQ, lowest_cycle, second_lowest_cycle)
            # ---
            
            # Step 5 ---- Crop
            Contention_Kahuna.cropPhase(valid_matchesQ, lowest_cycle);
            # ---

            times = times + 1; # Debug


        return None





    def Contention_FMU(self, valid_matchesQ: typing.List[MQ_Match], invalid_matchesQ: typing.List[MQ_Match])-> int:

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
        '''
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
        '''

        #print("Processing contention complete.")
        return readyMatchID;

