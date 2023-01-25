from Topology import *
from FMU_CircularBuffer import *
from Contention_FlexibleMemoryUnit import *





class TopFreeMemoryUnit(Topology):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopFreeMemoryUnit, self).__init__(nRanks, configfile);
        self.nFMUs = configfile.number_of_FMUs;
        self.independent_send_recv = True;
        assert self.nFMUs > 0, "Number of Free Memory Units needs to be at least 1 when using FMUs topology"

        #Override
        self.interLatency = configfile.fmu_latency;
        self.interBandwidth = configfile.fmu_bandwidth;

        self.top_fmu: Contention_FlexibleMemoryUnit;
        self.top_fmu = Contention_FlexibleMemoryUnit.getMeTheContentionMethod(nRanks, configfile);

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = self.top_fmu.fmu_circularBuffer;
        self.fmu_congestion_time = self.top_fmu.fmu_congestion_time;
        self.nFMUs = self.top_fmu.nFMUs;


    def processContention(self, matchQ: typing.List[MQ_Match])-> MQ_Match:
        return self.top_fmu.processContention(matchQ);

