#!/bin/bash
trap "kill 0" EXIT 

echo "experiments for different sample rate and different sample strategy in calculating F-score"

for s_rate in 0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7
	do
        echo "Running query of f1 sample rate = ${s_rate}"
        samplepct=echo "${s_rate}*100" | bc
        # echo "experiments.py -M 2 -F 0.1 -i f -m 0 -P 5433 -D feb9_weighted_${s_rate*100}_percent_sample -U postgres -p lcj53242 -d nba_study -t e"
        python experiments.py -M 2 -F 0.1 -i f -m 0 -P 5433 -D feb9_weighted_${samplepct}_percent_sample -U postgres -p lcj53242 -d nba_study -t e

        # python3 experiments.py -M $2 -p lcj53242 -U lchenjie -P 5433 -d nba -t e -F ${s_rate} -D $1
    done