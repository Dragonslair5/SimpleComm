


def getDataTypeSize(datatype: int) -> int:
    if datatype == 0: # MPI_DOUBLE
        return 8;
    if datatype == 1: # MPI_INT
        return 4;
    if datatype == 2: # MPI_CHAR
        return 1;
    if datatype == 23: # MPI_INTEGER32
        return 4;
    assert datatype == 0, "Unknown datatype " + str(datatype)


#MPI_DATATYPE = [8, # 0 - MPI_DOUBLE
#                4, # 1 - MPI_INT
#                1  # 2 - MPI_CHAR
#                ]


# (MPIC)onstants
MPIC_SEND=0
MPIC_RECV=1

actions = ["init",
"finalize",
"comm_size",
"comm_split",
"comm_dup",
"send",
"isend",
"recv",
"irecv",
"test",
"wait",
"waitall",
"barrier",
"bcast",
"reduce",
"allreduce",
"alltoall",
"alltoallv",
"gather",
"scatter",
"gatherv",
"scatterv",
"allgather",
"allgatherv",
"reducescatter",
"compute",
"sleep",
"location"]


# send <trg> <?> <#amount> <MPI_DATATYPE>
# recv <src> <?> <#amount> <MPI_DATATYPE>
# bsend <#amount> <root> <MPI_DATATYPE>


'''
class SendRecv:
    def __init__(self, kind, rank, partner, size, baseCycle, operation_origin = "Unknown"):
        self.kind=kind; # SEND or RECV
        self.rank = rank;
        self.partner = partner;
        self.size = size;
        self.baseCycle = baseCycle;

    def __str__(self):
        if self.kind == MPIC_SEND:
            return str(self.rank) + " SEND to " + str(self.partner);
        elif self.kind == MPIC_RECV:
            return str(self.rank) + " RECV from " + str(self.partner);
        return "Unknown SendRecv " + str(self.kind)
'''

'''        
        match self.kind:
            case MPIC_SEND:
                return str(self.rank) + " SEND to " + str(self.partner);
            case MPIC_SEND:
                return str(self.rank) + " RECV from " + str(self.partner);
            default:
                return "Unknown SendRecv";
'''


class MQ_Match:
    def __init__(self, rankS, rankR, size, baseCycle, endCycle, tag = None, blocking_send = True, blocking_recv = True, send_origin = "", recv_origin = "", positionS = 0, positionR = 0):
        self.rankS = rankS;
        self.rankR = rankR;
        self.size = size;
        self.baseCycle = baseCycle;
        self.endCycle = endCycle;
        self.tag = tag;
        self.blocking_send = blocking_send;
        self.blocking_recv = blocking_recv;
        self.send_origin = send_origin;
        self.recv_origin = recv_origin;
        self.positionS = positionS; # Ordering
        self.positionR = positionR; # Ordering


    def __str__ (self):
        return "[(" + str(self.positionS) + ")S:" + str(self.rankS) + " (" + str(self.positionR) + ")R:" + str(self.rankR) + "]"

        #print(str(self.rankS) + " -> " + str(self.rankR))


'''
class MQ_bcast_entry:
    def __init__ (self, rank, root, size, baseCycle):
        self.rank = rank;
        self.root = root;
        self.size = size;
        self.baseCycle = baseCycle;


class MQ_Bcast:
    def __init__(self, num_ranks, root, size):
        self.num_ranks = num_ranks;
        self.entries = []
        self.root = root;
        self.size = size;
        self.baseCycle = 0;
        
    def incEntry(self, bcast_entry):
        self.entries.append(bcast_entry)
        #print("bcast_entry at: " + str(bcast_entry.baseCycle))
        # self.ranks.append(rank);
        if self.baseCycle < bcast_entry.baseCycle:
            self.baseCycle = bcast_entry.baseCycle;

    def isReady(self):
        return self.num_ranks == len(self.entries)
        #return self.ready == self.num_ranks;

    def process(self):
        assert self.num_ranks == len(self.entries)
        sr_list = [];
        for rank in range(self.num_ranks):
            mask = 0x1;
            #print(mask)
            #relative_rank = (rank >= self.root) if rank - self.root else rank - self.root + self.num_ranks;
            if rank >= self.root:
                relative_rank = rank - self.root;
            else:
                relative_rank = rank - self.root + self.num_ranks;

            #print("Rank "+str(rank) + " relative rank "+str(relative_rank) + " root " + str(self.root))
            while mask < self.num_ranks:
                if relative_rank & mask:
                    src = rank - mask;
                    if src < 0:
                        src = src + self.num_ranks;
                    #print("Rank " + str(rank) + " received from " + str(src));
                    sr = SendRecv(MPIC_RECV, rank, src, self.size, self.baseCycle);
                    sr_list.append(sr);
                    break;
                mask = mask << 1;
                #print(mask)

            mask = mask >> 1;
            #print(mask)
            while mask > 0:
                if relative_rank + mask < self.num_ranks:
                    dst = rank + mask;
                    if dst >= self.num_ranks:
                        dst = dst - self.num_ranks;
                    sr = SendRecv(MPIC_SEND, rank, dst, self.size, self.baseCycle);
                    sr_list.append(sr);
                    #print("Rank " + str(rank) + " sends to " + str(dst));
                mask = mask >> 1;
                #print(mask)

        #for i in range(len(sr_list)):
        #    print(sr_list[i])

        return sr_list;
'''


'''
class CO_Bcast:
    def __init__(self, rank, baseCycle, root, size):
        self.rank = rank;
        self.baseCycle = baseCycle;
        self.root = root;
        self.size = size;
'''
