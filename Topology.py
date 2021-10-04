from Rank import *

# This function is like a placeholder
# TODO: expand this
def SimpleCommunicationCalculus(workload):
    workload = int(workload) + 16
    latency=0;
    bandwidth=1;
    #return 10
    return latency + workload/bandwidth;



def processContention(matchQ, earliest_match: MQ_Match, topology):
        
        if (topology == "SC_CC"):    
            # This is the actual SINGLE CHANNEL CIRCUIT SWITCHING
            # Push forward everyone that shares communication with the earliest
            for mi in range( len(matchQ) ):
                inc = earliest_match.endCycle - matchQ[mi].baseCycle
                if inc > 0:
                    matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                    matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
            return None;
        
        if (topology == "SC_FATPIPE"):
            # Alltoall FATPIPE here
            rank_send = earliest_match.rankS;
            rank_recv = earliest_match.rankR;
            for mi in range( len(matchQ) ):
                if ( (matchQ[mi].rankS - rank_send) * (matchQ[mi].rankS - rank_recv) * (matchQ[mi].rankR - rank_send) * (matchQ[mi].rankR - rank_recv) ) == 0:
                #if (  (matchQ[mi].rankS - rank_recv) * (matchQ[mi].rankR - rank_send) ) == 0:
                #if (  (matchQ[mi].rankS - rank_send) ) == 0:
                    inc = earliest_match.endCycle - matchQ[mi].baseCycle;
                    if inc > 0:
                        matchQ[mi].baseCycle = matchQ[mi].baseCycle + inc;
                        matchQ[mi].endCycle = matchQ[mi].endCycle + inc;
                        #if matchQ[mi].removelat:
                        #    matchQ[mi].endCycle = matchQ[mi].endCycle - 1;
                        #    matchQ[mi].removelat = False;
            return None;
            
        print( bcolors.FAIL + "ERROR: Unknown topology " + topology + bcolors.ENDC);
        sys.exit(1);