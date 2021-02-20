#!/bin/bash
# medicare vs private: effects of sampling
trap "kill 0" EXIT 

echo "experiments for different sample rate and different sample strategy in calculating F-score"

samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
recthreshs=(0.3 0.5 0.7 0.9)

for s in ${samplerates[@]}
	do 
		for r in ${recthreshs[@]}
		do
		    python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t s -m ${r} -i false -F ${s} -D feb17_sample_and_recall
	    done
	done


