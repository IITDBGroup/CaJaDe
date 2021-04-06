#!/bin/bash
# medicare vs private: effects of sampling
trap "kill 0" EXIT 

echo "experiments scability on NBA"

array=('01' '05' '2' '4' '8')

rates=(0.1 0.3 0.5 0.7)

for s in ${array[@]}
	do
		for r in ${rates[@]}
			do
        # echo "Running query of f1 sample rate = ${s}"
	        python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev_${s} -t s -m 0 -i false -F ${r} -D april_3_nba_scalability
	    done
    done



for r in ${rates[@]}
	do
	# echo "Running query of f1 sample rate = ${s}"
    python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_original -t s -m 0 -i false -F ${r} -D april_3_nba_scalability
    done
