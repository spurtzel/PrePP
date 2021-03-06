#!/bin/bash

cd ..      
for j in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0
do
		a=0
		while [ $a -lt 50 ]
		do
		python3.6 generate_network.py 20 $j
		python3.6 write_config_single.py
		python3.6 determine_all_single_selectivities.py
		python3.6 generate_projections.py
        python3.6 combigen.py
		python3.6 computePlanCosts.py eventNodeRatio
		python3.6 generateEvaluationPlan.py
		a=`expr $a + 1`
		done
done
