#!/bin/bash

cd ..         
for j in 10 20 50 75 100 150 200 250
do
		a=0
		while [ $a -lt 50 ]
		do
		python3.6 generate_network.py $j 0.5 1.3 10
		python3.6 write_config_single.py
		python3.6 generateEvaluationPlan.py "$j"_"$a" 
		a=`expr $a + 1`
		done
done
