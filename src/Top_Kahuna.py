from Topology import *
from Contention_Kahuna import *

class TopKahuna(Topology):

    def __init__(self, nRanks, configfile: SimpleCommConfiguration):
        super(TopKahuna, self).__init__(nRanks, configfile);

        self.contentionObject: Contention_Kahuna;
        self.contentionObject = Contention_Kahuna(nRanks, configfile);


    def processContention(self, matchQ: typing.List[MQ_Match])-> MQ_Match:
        return self.contentionObject.processContention(matchQ);