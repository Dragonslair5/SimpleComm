from Rank import *
from abc import ABC, abstractmethod



# Abstract class
class Topology(ABC):

    #def __init__(self, nRanks, topology, interLatency, interBandwidth, intraLatency, intraBandwidth):
    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        self.nRanks = nRanks;
        self.cores_per_node = 1; # TODO: Fix it after you include this option
        self.topology = configfile.topology;
        self.interLatency = configfile.internode_latency;
        self.interBandwidth = configfile.internode_bandwidth;
        self.intraLatency = configfile.intranode_latency;
        self.intraBandwidth = configfile.intranode_bandwidth;
        self.independent_send_recv = False;
        self.eager_protocol_max_size = configfile.eager_protocol_max_size;

    
    


    def CommunicationCalculus_Bandwidth(self, rankS: int, rankR: int, workload: int):

        workload = int(workload) + 16; # 16 Bytes as MPI overhead (based on SimGrid)

        nodeS = rankS // self.cores_per_node;
        nodeR = rankR // self.cores_per_node;

        bandwidth: float;
        if nodeS == nodeR: # Intranode
            bandwidth=self.intraBandwidth;
        else: #Internode
            bandwidth=self.interBandwidth;

        if bandwidth == 0:
            return 0, bandwidth;
        else:
            return workload/bandwidth, bandwidth;

    def CommunicationCalculus_Latency(self, rankS: int, rankR: int, workload: int):

        if rankS == rankR:
            return 0;

        nodeS = rankS // self.cores_per_node;
        nodeR = rankR // self.cores_per_node;

        latency: float;
        if nodeS == nodeR: # Intranode
            latency = self.intraLatency;
        else: # Internode
            latency = self.interLatency;

        return latency;
        
    



    '''
    def SimpleCommunicationCalculusInternode(self, workload):
        workload = int(workload) + 16 # 16 Bytes as MPI overhead (based on SimGrid)
        #latency=self.interLatency; # Treating latency elsewhere
        bandwidth=self.interBandwidth;
        return workload/bandwidth;
        #return latency + workload/bandwidth;

    def SimpleCommunicationCalculusIntranode(self, workload):
        workload = int(workload) + 16 # 16 Bytes as MPI overhead (based on SimGrid)
        #latency=self.interLatency; # Treating latency elsewhere
        bandwidth=self.interBandwidth;
        return 0;
        return latency + workload/bandwidth;
    '''

    @abstractmethod
    def processContention(self, matchQ) -> MQ_Match:
        pass;

    
    def separateValidAndInvalidMatches(
        self, 
        matchQ: typing.List[MQ_Match], 
        col_matchQ: typing.List[MQ_Match], 
        currentPosition: typing.List[int]  
    ) -> typing.Tuple[ typing.List[MQ_Match] , typing.List[MQ_Match]]:

        valid_matchesQ : list[MQ_Match]; # For valid matches
        valid_matchesQ = []
        invalid_matchesQ : list[MQ_Match]; # For invalid matches
        invalid_matchesQ = []

        # Find the valid matches
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

        return valid_matchesQ, invalid_matchesQ;


    #@DeprecationWarning
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