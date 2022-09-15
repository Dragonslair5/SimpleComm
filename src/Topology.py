from Rank import *
from abc import ABC, abstractmethod



# Abstract class
class Topology(ABC):

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

