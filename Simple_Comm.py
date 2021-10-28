#! /usr/bin/python3

import sys

from SimulationEngine import *


def main():

    if len (sys.argv) < 4:
        print("Usage: python3 simple_comm.py <#ranks> <path for traces> <config file> <? verbose ?>");
        sys.exit(1);

    verbose = False;
    if len (sys.argv) > 4:
        verbose = True;

    nRanks=int(sys.argv[1]);
    files_path=sys.argv[2];
    configFile = sys.argv[3];


    # StartUp the simulation engine
    simEngine = SimpleCommEngine(nRanks, configFile, verbose);

    # Read configurations
    #simEngine.configure(configFile)

    # Read up the traces
    simEngine.read_traces(nRanks, files_path);

    # Execute the simulation
    run = 0;
    while run < nRanks:
        run = simEngine.simulate();
        simEngine.showResults();

    #simEngine.showResults();

    #printColors();

if __name__ == "__main__":
    main()


