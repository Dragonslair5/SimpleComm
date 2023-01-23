from Topology import *
from FMU_CircularBuffer import *
from Contention_FlexibleMemoryUnit import *





class TopFreeMemoryUnit(Topology):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopFreeMemoryUnit, self).__init__(nRanks, configfile);
        self.nFMUs = configfile.number_of_FMUs;
        self.independent_send_recv = True;
        assert self.nFMUs > 0, "Number of Free Memory Units needs to be at least 1 when using FMUs topology"
        #assert self.eager_protocol_max_size == 0, "Eager Protocol can not be activated with this Topology"

        #Override
        self.interLatency = configfile.fmu_latency;
        self.interBandwidth = configfile.fmu_bandwidth;

        self.contentionObject: Contention_FlexibleMemoryUnit;
        self.contentionObject = Contention_FlexibleMemoryUnit.getMeTheContentionMethod(nRanks, configfile);

        self.fmu_circularBuffer : FMU_CircularBuffer;
        self.fmu_circularBuffer = self.contentionObject.fmu_circularBuffer;
        self.fmu_congestion_time = self.contentionObject.fmu_congestion_time;
        self.nFMUs = self.contentionObject.nFMUs;


    def processContention(self, matchQ: typing.List[MQ_Match])-> MQ_Match:
        return self.contentionObject.processContention(matchQ);

