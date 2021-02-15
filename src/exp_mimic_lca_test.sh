#!/bin/bash
# 15 vs 12: effects of sampling
trap "kill 0" EXIT 

echo "experiments for LCA size effects on time and quality"

# original
python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false -F 0.05 -D feb12_mimic_lca_only
python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false -F 0.05 -D feb12_mimic_lca_only
python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false -F 0.05 -D feb12_mimic_lca_only

mins=(10 20 30 40 50)

maxs=(100 200 300 400 500 600 700 800 1000)

for s in ${mins[@]}
	do 
		for x in ${maxs[@]}
		do
			echo "python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -i false -F 0.5 -s ${s} -S ${x} -D feb12_mimic_lca_only"
		    python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -i false -F 0.5 -s ${s} -S ${x} -D feb12_mimic_lca_only
	    done
	done
