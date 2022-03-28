# Plan Generators & PrePP Algorithms

## Plan generators
Scripts for reproducing inputfiles for experiments presented in paper.
Using the `_plan.sh`-version of a script, saves for each experiment the resulting INEv graphs in the `plans/` folder. 
Please note, that only only one script can be executed at a time.

- *\_single and *\_single\_plan execute set of experiments for single query of size 7, concrete event rates used in networks for experiments in paper can be found in top section of generate_network.py 
- eventSkewLatency.sh execute set of experiments for query workload of size 10, and varying latency factors
- conflicting_qwl to reproduce performance of multi-query scenario with varying overlap in event types between queries
- *\_qwl and *\_qwl\_plan execute set of experiments for query workloads of varying sizes, concrete event rates used in networks for experiments in paper can be found in top section of generate_network.py 

## Parameters for PrePP script
Example: for executing the `prepp.py` script with the inputfile `single_query.txt` for a centralized PrePP plan using the sampling algorithm with s = 1024, top-k = 10, 500 runs, and printing resulting plans. Outputs are saved into `output.txt`:
`python3 prepp.py single_query cent s 1024 10 500 t > output.txt`

Example: for executing the `prepp.py` script with the inputfile `multi_query_muse.txt` for a P-P MuSE graph using the exact algorithm with s = 0, top-k = 0, 500 runs, and not printing resulting plans. Outputs are saved into `output.txt`:
`python3 prepp.py multi_query_muse ppmuse e 0 0 500 f > output.txt`

Example: for executing the `prepp.py` script with the inputfile `single_query.txt` for a centralized PrePP plan using the factorial algorithm (top-k == best-k single-step PrePP plans) with s = 0, top-k = 10, 500 runs, and printing resulting plans. Outputs are saved into `output.txt`:
`python3 prepp.py single_query cent f 0 10 500 t > output.txt`

Example: for executing the `prepp.py` script with the inputfile `multi_query_muse.txt` for a P-P MuSE graph using the greedy algorithm with s = 0, top-k = 0, 500 runs, and not printing resulting plans. Outputs are saved into `output.txt`:
`python3 prepp.py multi_query_muse ppmuse g 0 0 500 f > output.txt`

### Parameters for PrePP script
Parameter | Meaning
------------ | -------------
1. filename | required inputfile name, i.e., without ".txt" at the end
2. cent or ppmuse | for centralized PrePP plans or, given a MuSE input, for computing P-P MuSE graphs
3. g e f s |  g == greedy, e == exact, f == factorial approx, s == sampling
4. #samples |  number of samples, only for sampling algorithm
5. k |  value for top-k parameter, i.e., only for factorial and sampling algorithm
6. #runs |  number of runs the experiment is repeated
7. t or f |  printing plans true, or false
