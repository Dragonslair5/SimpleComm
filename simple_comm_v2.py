#! /usr/bin/python3

import sys

from tp_utils import *
from MPI_Constants import *
from Rank import *
from SimulationEngine import *


def main():

    if len (sys.argv) < 3:
        print("Usage: python3 simple_comm.py <#ranks> <path for traces>");
        sys.exit(1);


    nRanks=int(sys.argv[1]);
    files_path=sys.argv[2];


    # StartUp the simulation engine
    simEngine = SimpleCommEngine();

    # Read up the traces
    simEngine.read_traces(nRanks, files_path);

    # Execute the simulation
    run = 0;
    while run < nRanks:
        run = simEngine.simulate();

    simEngine.showResults();


if __name__ == "__main__":
    main()


