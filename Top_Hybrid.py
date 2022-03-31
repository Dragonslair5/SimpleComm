from Topology import *
from FMU_CircularBuffer import *
from Contention_Kahuna import *




# The initial intent of this Topology is to be a topology of topologies.
class TopHybrid(Topology):


    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopHybrid, self).__init__(nRanks, configfile);

        self.pivotValue = configfile.fmu_pivot_value;
        self.fmu_bandwidth = configfile.fmu_bandwidth;
        self.fmu_latency = configfile.fmu_latency;

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = FMU_CircularBuffer(self.nFMUs);


    
    # Decide if a match should be served by FMU
    def isThroughFMU(self, rankS: int, rankR: int, size: int):

        if size > self.pivotValue:
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

        # Now we have
            #invalid_matchesQ
            #network_matchesQ
            #fmu_matchesQ


        # Initialize matches on FMU
        for i in range(len(fmu_matchesQ)):
            if not fmu_matchesQ[i].initialized:
                fmu_matchesQ[i].sep_initializeMatch(self.CommunicationCalculus_Bandwidth(fmu_matchesQ[i].rankS, fmu_matchesQ[i].rankR, fmu_matchesQ[i].size))
        
    

        lowest_cycle_fmu: float;
        lowest_cycle_fmu = None;
        lowest_cycle_network: float;
        lowest_cycle_network = None;

        if len(fmu_matchesQ) > 0:
            lowest_cycle_fmu = fmu_matchesQ[0].sep_getBaseCycle();

        if len(network_matchesQ) > 0:
            lowest_cycle_network = Contention_Kahuna.findWindow(network_matchesQ);

        if lowest_cycle_fmu <= lowest_cycle_network:
            # Execute FMU
            pass
        else:
            # Execute network until ReadyMatch OR until lowest_cycle_fmu
            # Maybe we could ignore the second condition for a first version
            pass


        



        return None;



