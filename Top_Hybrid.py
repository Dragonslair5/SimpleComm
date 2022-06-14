from Topology import *
from FMU_CircularBuffer import *
from Contention_Kahuna import *
from Contention_FlexibleMemoryUnit import *




# The initial intent of this Topology is to be a topology of topologies.
class TopHybrid(Topology):


    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopHybrid, self).__init__(nRanks, configfile);

        self.fmu_latency = configfile.fmu_latency;
        self.fmu_bandwidth = configfile.fmu_bandwidth;

        # Constants here
        self.TOP_FMU = 1;
        self.TOP_NETWORK = 2;
        # ****

        self.total_messages = 0;
        self.total_messages_fmu = 0;
        self.total_messages_network = 0;

        self.used_topology = None;

        #self.pivotValue = configfile.fmu_pivot_value;
        self.pivotValue = self.calculatePivot(self.interLatency, self.interBandwidth, self.fmu_latency, self.fmu_bandwidth);


        self.top_kahuna = Contention_Kahuna(nRanks, configfile);
        self.top_fmu = Contention_FlexibleMemoryUnit(nRanks, configfile);

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = self.top_fmu.fmu_circularBuffer;
        self.fmu_congestion_time = self.top_fmu.fmu_congestion_time;
        self.nFMUs = self.top_fmu.nFMUs;



    # Decide if a match should be served by FMU
    def isThroughFMU_preMatch(self, rankS: int, rankR: int, size: int)->bool:
    
        # Negative value means to use only the network
        if self.pivotValue < 0:
            return False;
    
        if size >= self.pivotValue:
            return True;
        return False


    # Decide if a match should be served by FMU
    def isThroughFMU(self, match: MQ_Match)->bool:

        size = match.size;
        # Negative value means to use only the network
        if self.pivotValue < 0:
            return False;

        if size >= self.pivotValue:
            return True;
        return False



    # -1 = Network only
    #  0 = FMU only
    def calculatePivot(self, network_latency: float, network_bandwidth: float, fmu_latency: float, fmu_bandwidth: float)->float:

        fmu_latency = fmu_latency * 2;

        # If latencies are equal
        if(network_bandwidth == fmu_bandwidth):
            if fmu_latency < network_latency:
                return 0.0;
            else:
                return -1.0;

        # If bandwidths are equal
        if(network_latency == fmu_latency):
            if fmu_bandwidth > network_bandwidth:
                return 0.0;
            else:
                return -1.0;

        # if lat and bw are different
        pivotValue = (fmu_latency - network_latency) / ((1/network_bandwidth) - (1/fmu_bandwidth))

        if pivotValue < 0:
            if fmu_bandwidth > network_bandwidth:
                return 0.0;
            else:
                return -1.0;

        if fmu_bandwidth > network_bandwidth:
            return pivotValue;


        assert False, "FMU is worst than Network for large messages, but better on small messages. We did not implement this case."


    # Override
    def dep_CommunicationCalculus_Bandwidth(self, rankS: int, rankR: int, workload: int):

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


    def dep_CommunicationCalculus_Latency(self, rankS: int, rankR: int, workload: int):

        if rankS == rankR:
            return 0;

        nodeS = rankS // self.cores_per_node;
        nodeR = rankR // self.cores_per_node;

        latency: float;
        if nodeS == nodeR: # Intranode
            latency = self.intraLatency;
        else: # Internode
            if self.isThroughFMU(rankS, rankR, workload):
                latency = self.fmu_latency;
            else:
                latency = self.interLatency;

        return latency;


    # Override
    def CommunicationCalculus_Bandwidth(self, rankS: int, rankR: int, workload: int):
        
        workload = int(workload) + 16; # 16 Bytes as MPI overhead (based on SimGrid)

        nodeS = rankS // self.cores_per_node;
        nodeR = rankR // self.cores_per_node;

        bandwidth: float;
        if nodeS == nodeR: # Intranode
            bandwidth=self.intraBandwidth;
        else: #Internode
            if self.isThroughFMU_preMatch(rankS, rankR, workload):
                return self.top_fmu.CommunicationCalculus_Bandwidth(rankS, rankR, workload);
            else:
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
            if self.isThroughFMU_preMatch(rankS, rankR, workload):
                #latency = self.fmu_latency;
                return self.top_fmu.CommunicationCalculus_Latency(rankS, rankR, workload);
            else:
                latency = self.interLatency;

        return latency;


    def processContention(self, matchQ: typing.List[MQ_Match]) -> MQ_Match:

        network_matchesQ : list[MQ_Match];
        network_matchesQ = [];
        fmu_matchesQ : list[MQ_Match];
        fmu_matchesQ = [];

        for i in range(len(matchQ)):
            match = matchQ[i];
            if self.isThroughFMU(match):
                fmu_matchesQ.append(match)
            else:
                network_matchesQ.append(match);
        
        # *** Find Lowest
        lowest_cycle_fmu: float;
        lowest_cycle_fmu = None;
        lowest_cycle_network: float;
        lowest_cycle_network = None;

        # Check for not initialized matches, and initialize them
        for i in range(len(fmu_matchesQ)):
            if not fmu_matchesQ[i].initialized:
                fmu_matchesQ[i].sep_initializeMatch(self.top_fmu.CommunicationCalculus_Bandwidth(fmu_matchesQ[i].rankS, fmu_matchesQ[i].rankR, fmu_matchesQ[i].size)[0])


        if len(fmu_matchesQ) > 0:
            lowest_cycle_fmu = fmu_matchesQ[0].sep_getBaseCycle();
            for i in range(len(fmu_matchesQ)):
                if lowest_cycle_fmu > fmu_matchesQ[i].sep_getBaseCycle():
                    lowest_cycle_fmu = fmu_matchesQ[i].sep_getBaseCycle();

        if len(network_matchesQ) > 0:
            readyMatch = self.top_kahuna.findReadyMatch(network_matchesQ);
            if readyMatch is None:
                lowest_cycle_network = self.top_kahuna.findWindow(network_matchesQ)[0];
            else:
                lowest_cycle_network = readyMatch.baseCycle;

        
        # ****************************************************************************

        #readyMatchID: int;
        #readyMatchID = None;
        readyMatch : MQ_Match;
        readyMatch = None;

        if lowest_cycle_fmu == None:
            #readyMatchID = self.Contention_Kahuna(network_matchesQ, invalid_matchesQ, -1);
            readyMatch = self.top_kahuna.processContention(network_matchesQ);
            self.total_messages_network = self.total_messages_network + 1;
        else:
            if lowest_cycle_network == None:
                #readyMatchID = self.Contention_FMU(fmu_matchesQ, invalid_matchesQ);
                readyMatch = self.top_fmu.processContention(fmu_matchesQ);
                self.total_messages_fmu = self.total_messages_fmu + 1;
            else:
                if lowest_cycle_fmu <= lowest_cycle_network:
                    #readyMatchID = self.Contention_FMU(fmu_matchesQ, invalid_matchesQ);
                    readyMatch = self.top_fmu.processContention(fmu_matchesQ);
                    self.total_messages_fmu = self.total_messages_fmu + 1;
                else:
                    #readyMatchID = self.Contention_Kahuna(network_matchesQ, invalid_matchesQ, lowest_cycle_fmu);
                    readyMatch = self.top_kahuna.processContention(network_matchesQ);
                    self.total_messages_network = self.total_messages_network + 1;
                    #if readyMatchID == None:
                    #    readyMatchID = self.Contention_FMU(fmu_matchesQ, invalid_matchesQ);

        assert readyMatch is not None, "what?"

        readyMatchID = readyMatch.id;
        readyMatch = None;
        for i in range(len(matchQ)):
            if readyMatchID == matchQ[i].id:
                readyMatch = matchQ.pop(i)
                break;
        
        assert readyMatch is not None, "ready match is not presented on matches queues"


        self.total_messages = self.total_messages + 1;

        return readyMatch;


        return None;

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

        #for i in range(len(valid_matchesQ)):
        #    match = valid_matchesQ[i];
        #    fmu_matchesQ.append(match)

        for i in range(len(valid_matchesQ)):
            match = valid_matchesQ[i]
            if self.isThroughFMU(match.rankS, match.rankR, match.size):
                fmu_matchesQ.append(match);
                pass
            else:
                network_matchesQ.append(match);
                pass


        # *** Find Lowest
        lowest_cycle_fmu: float;
        lowest_cycle_fmu = None;
        lowest_cycle_network: float;
        lowest_cycle_network = None;


        # Check for not initialized matches, and initialize them
        for i in range(len(fmu_matchesQ)):
            if not fmu_matchesQ[i].initialized:
                fmu_matchesQ[i].sep_initializeMatch(self.CommunicationCalculus_Bandwidth(fmu_matchesQ[i].rankS, fmu_matchesQ[i].rankR, fmu_matchesQ[i].size)[0])

        if len(fmu_matchesQ) > 0:
            lowest_cycle_fmu = fmu_matchesQ[0].sep_getBaseCycle();
            for i in range(len(fmu_matchesQ)):
                if lowest_cycle_fmu > fmu_matchesQ[i].sep_getBaseCycle():
                    lowest_cycle_fmu = fmu_matchesQ[i].sep_getBaseCycle();

        if len(network_matchesQ) > 0:
            lowest_cycle_network = Contention_Kahuna.findWindow(network_matchesQ)[0];

        #**************************************************************


        readyMatchID: int;
        readyMatchID = None;

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



        #**************************************************************


        #readyMatchID = self.Contention_Kahuna(network_matchesQ, invalid_matchesQ, -1);
        #readyMatchID = self.Contention_FMU(fmu_matchesQ, invalid_matchesQ);

        # Grab the ready match from the matches queue (matchQ) or collectives matches queue (col_matchQ)
        readyMatch: MQ_Match;
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




        if self.used_topology == self.TOP_NETWORK:
            readyMatch.send_endCycle = readyMatch.endCycle;
            readyMatch.recv_endCycle = readyMatch.endCycle
            # Eager Protocol
            if readyMatch.size < self.eager_protocol_max_size:
                readyMatch.send_endCycle = readyMatch.send_baseCycle;
                if readyMatch.recv_baseCycle > readyMatch.endCycle:
                    readyMatch.recv_endCycle = readyMatch.recv_baseCycle;
            self.total_messages_network = self.total_messages_network + 1;
        elif self.used_topology == self.TOP_FMU:
            # Eager Protocol
            if readyMatch.size < self.eager_protocol_max_size:
                readyMatch.send_endCycle = readyMatch.send_baseCycle;
            readyMatch.endCycle = readyMatch.recv_endCycle;
            self.total_messages_fmu = self.total_messages_fmu + 1;
        else:
            print( bcolors.FAIL + "ERROR: Unknown Topology being used on Hybrid topology" + bcolors.ENDC);
            sys.exit(1);


        self.total_messages = self.total_messages + 1;

        #print("endS: " + str(readyMatch.send_endCycle) + " endR: " + str(readyMatch.recv_endCycle) + " end: " + str(readyMatch.endCycle) )



        return readyMatch;