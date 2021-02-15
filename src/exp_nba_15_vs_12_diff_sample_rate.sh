#!/bin/bash
# 15 vs 12: effects of sampling
trap "kill 0" EXIT 

echo "experiments for different sample rate and different sample strategy in calculating F-score"

# original
# python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false -F 0.05 -D feb11_nba_15_vs_12_sample_rate_eval
python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false -F 0.05 -D feb11_nba_15_vs_12_sample_rate_eval
python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false -F 0.05 -D feb11_nba_15_vs_12_sample_rate_eval

samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
maxedges=(1 2 3)

for s in ${samplerates[@]}
	do 
		for e in ${maxedges[@]}
		do
			# echo "python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -i false -F ${s} -D feb11_nba_15_vs_12_sample_rate_eval"
		    python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -i false -F ${s} -D feb11_nba_15_vs_12_sample_rate_eval
	    done
	done
