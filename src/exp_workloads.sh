#!/bin/bash

iters=(5)

for i in ${iters[@]}
	do
		python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_original -t s -m 0 -F 0.3 -D april_5_workload_rep_${i} -W true
		python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_original -t s -m 0 -F 0.3 -D april_5_workload_rep_${i} -W true
	done
