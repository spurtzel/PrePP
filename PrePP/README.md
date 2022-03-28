# PrePP Algorithms



## Scripts
Scripts for reproducing experiments presented in paper.
Using the `_plan.sh`-version of a script, saves for each experiment the resulting INEv graphs in the `plans/` folder. 
Please note, that only only one script can be executed at a time.

- *\_single and *\_single\_plan execute set of experiments for single query of size 7, concrete event rates used in networks for experiments in paper can be found in top section of generate_network.py 
- eventSkewLatency.sh execute set of experiments for query workload of size 10, and varying latency factors
- conflicting_qwl to reproduce performance of multi-query scenario with varying overlap in event types between queries
- *\_qwl and *\_qwl\_plan execute set of experiments for query workloads of varying sizes, concrete event rates used in networks for experiments in paper can be found in top section of generate_network.py 


### Parameters for PrePP script
Parameter | Meaning
------------ | -------------
1. filename | required inputfile name, i.e., without ".txt" at the end
2. cent or ppmuse | 
3. g e f s |  g == greedy, e == exact, f == factorial approx, s == sampling
4. #samples |  number of samples, only for sampling algorithm
5. k |  value for top-k parameter, i.e., only for factorial and sampling algorithm
6. #runs |  number of runs the experiment is repeated
7. t or f |  printing plans true, or false
