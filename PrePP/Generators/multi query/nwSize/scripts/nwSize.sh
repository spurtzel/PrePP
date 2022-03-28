#!/bin/sh

cd ..
for j in  10 20 50 75 100 150 200 250
do
		a=0
		while [ $a -lt 50 ]
		do
		python3.6 generate_network.py $j
		python3.6 generate_graph.py
		python3.6 allPairs.py
		python3.6 write_config_single.py
		python3.6 determine_all_single_selectivities.py
		python3.6 generate_projections.py
		python3.6 combigen.py
		python3.6 computePlanCosts.py nwSize
		python3.6 generateEvaluationPlan.py
		a=`expr $a + 1`
		done
done
