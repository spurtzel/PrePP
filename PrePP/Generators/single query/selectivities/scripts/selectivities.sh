#!/bin/sh
cd .. 
for j in  0.01 0.001 0.0001 0.00001 0.000001 
do
		a=0
		k=$(echo "scale=10;0.1 * $j" | bc)
		echo $k
		while [ $a -lt 50 ]
		do		
		python3.6 generate_selectivity.py $j $k
		python3.6 write_config_single.py

		python3.6 generateEvaluationPlan.py "$j"_"$a" 
		a=`expr $a + 1`
		done
done
