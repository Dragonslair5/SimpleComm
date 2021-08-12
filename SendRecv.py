from tp_utils import *
from MPI_Constants import *


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