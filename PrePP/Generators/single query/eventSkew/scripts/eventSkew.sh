#!/bin/bash

cd ..         
for j in 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 2.0
do
		a=0
		while [ $a -lt 50 ]
		do
		python3.6 generate_network.py 20 0.5 $j 10
		python3.6 write_config_single.py
		python3.6 generateEvaluationPlan.py "$j"_"$a" 
		a=`expr $a + 1`
		done
done
