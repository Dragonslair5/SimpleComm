from tp_utils import *
from MPI_Constants import *


class SendRecv:
    def __init__(self, kind, rank, partner, size, baseCycle, operation_origin = "Unknown", blocking = True, tag = None, col_id = 0):
        self.kind=kind; # SEND or RECV
        self.rank = rank;
        self.partner = partner;
        self.size = size;
        self.baseCycle = baseCycle;
        self.operation_origin = operation_origin;
        # Using on non-blocking operations
        self.blocking = blocking;
        self.tag = tag;
        self.queue_position = 0; # To be used by the MessageQueue for ordering
        self.col_id = col_id; # To be used for ordering Collective Operations that occurs with unblocking calls, to proper simulate latency.

    def __str__(self):
        if self.kind == MPIC_SEND:
            return str(self.rank) + " SEND to " + str(self.partner) + " -- size " + str(self.size) + " -- col_id " + str(self.col_id);
        elif self.kind == MPIC_RECV:
            return str(self.rank) + " RECV from " + str(self.partner) + " -- size " + str(self.size) + " -- col_id " + str(self.col_id);
        return "Unknown SendRecv " + str(self.kind)