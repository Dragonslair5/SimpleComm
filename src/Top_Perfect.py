from Topology import *

class TopPerfect(Topology):


    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopPerfect, self).__init__(nRanks, configfile);
        self.independent_send_recv = True;

    def CommunicationCalculus_Bandwidth(self, rankS: int, rankR: int, workload: int):
        return 0, 1e18; # 1e18 we consider a big number, as an "infinite" bandwidth.

    def CommunicationCalculus_Latency(self, rankS: int, rankR: int, workload: int):
        return 0;

    def processContention(self, matchQ: typing.List[MQ_Match])-> MQ_Match:

        readyMatch = matchQ.pop(0);

        # recv ready only after send
        if readyMatch.send_original_baseCycle > readyMatch.recv_original_baseCycle:
            readyMatch.recv_baseCycle = readyMatch.send_original_baseCycle;
        else:
            readyMatch.recv_baseCycle = readyMatch.recv_original_baseCycle;
        readyMatch.recv_endCycle = readyMatch.recv_baseCycle;

        # send is always ready (always EAGER PROTOCOL)
        readyMatch.send_endCycle = readyMatch.send_original_baseCycle;

        # The combined readyMatch
        # TODO: Check if this is needed.
        readyMatch.endCycle = readyMatch.baseCycle;

        return readyMatch;