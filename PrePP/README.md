# Plan Generators & PrePP Algorithms

## Inputfile generators
Scripts for reproducing inputfiles used for single query and multi query experiments presented in the paper.\
\
For producing the inputfiles for a varying event node ratio, follow the steps:\
Go into `../Generators/multi query/eventNodeRatio/scripts` and execute the `eventNode.sh` script. This will create 50 inputfiles for each parameter value from 0.1, 0.2, .. , 1.0. The resulting inputfiles are saved into `../eventNodeRatio/plans` and can be directly executed using the `PrePP` script as described below.

## PrePP plan generators
### Parameters for PrePP script
`python3 prepp.py 1 2 3 4 5 6 7 > output.txt`\
`python3 prepp.py single_query cent s 1024 10 500 t > output.txt`

|#|Parameter                                  |Meaning                  |
|-|-------------------------------------------|-----------------------|
|1| filename                                  |required inputfile name, i.e., without ".txt" at the end|
|2| centralized or P-P MuSE                   |for centralized PrePP plans or, given a MuSE input, for computing P-P MuSE graphs|
|3| algorithm                                 |g == greedy, e == exact, f == factorial approx, s == sampling
|4| #samples                                  |number of samples; only needed for sampling algorithm |
|5| top-k                                     |value for top-k parameter, i.e., only for factorial and sampling algorithm|
|6| #runs                                     |number of runs the experiment is repeated|
|7| printing plans                            |t == true, f == false|


## Examples for executing the PrePP script
Note: the `prepp.py` and `push_pull_plan_generator` scripts need to be in the same directory.\
\
Example 1: for executing the `prepp.py` script with the inputfile `single_query.txt` for a centralized PrePP plan using the sampling algorithm with s = 1024, top-k = 10, 500 runs, and printing resulting plans. Outputs are saved into `output.txt`:
`python3 prepp.py single_query cent s 1024 10 500 t > output.txt`

Example 2: for executing the `prepp.py` script with the inputfile `multi_query_muse.txt` for a P-P MuSE graph using the exact algorithm with s = 0, top-k = 0, 500 runs, and not printing resulting plans. Outputs are saved into `output.txt`:
`python3 prepp.py multi_query_muse ppmuse e 0 0 500 f > output.txt`

Example 3: for executing the `prepp.py` script with the inputfile `single_query.txt` for a centralized PrePP plan using the factorial algorithm (top-k == best-k single-step PrePP plans) with s = 0, top-k = 10, 500 runs, and printing resulting plans. Outputs are saved into `output.txt`:
`python3 prepp.py single_query cent f 0 10 500 t > output.txt`

Example 4: for executing the `prepp.py` script with the inputfile `multi_query_muse.txt` for a P-P MuSE graph using the greedy algorithm with s = 0, top-k = 0, 500 runs, and not printing resulting plans. Outputs are saved into `output.txt`:
`python3 prepp.py multi_query_muse ppmuse g 0 0 500 f > output.txt`



## Schema of Results
The script prints:
- central push costs
- costs for a particular run
- average costs over all runs
- average execution time
- average transmission ratio over all runs
