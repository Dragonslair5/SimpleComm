# Simple Comm


Trace-based performance simulator for MPI programs. 


## 🎁 Content

* __src/__ source code 
* __configFiles/__ configuration file template
* __traces/__ trace examples

## 💻 Requirements

Python 3.6.9 or higher.

[SimGrid](https://simgrid.org/) if you want to create the traces.


## ☕ Usage


[Simple_Comm.py](src/Simple_Comm.py)  is the main file of the simulator.

Execute with python3 in the following way:


```
python3 ./Simple_Comm.py $nRanks $tracePath $configFile [verbose]"
```

3 arguments are mandatory, the last one is optional.


* $nRanks is the number of ranks;
* $tracePath is the path to the folder that contains the trace files;
* $configFile is the configuration file that configures the simulator
* [verbose] is an optional argument that enables the verbose interactive output

Usage example:

```
python3 ./Simple_Comm.py 4 ../traces/toy_alltoall_4/ ./myConfigFile.ini verbose
```

The above example considers that you are in the /src folder.

If you include the verbose option, you will have to press ENTER several times to proceed with the execution.

