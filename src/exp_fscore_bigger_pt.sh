#!/bin/bash
trap "kill 0" EXIT 

echo "experiments for different sample rate in calculating F-score"

echo "schema desc=$1"

echo "max number of edges in jg: $2"

for s_rate in 0.1 0.3 0.5 0.7
	do
        echo "Running query of f1 sample rate = ${s_rate}"
        python3 experiments.py -M $2 -p lcj53242 -U lchenjie -P 5433 -d nba -t e -F ${s_rate} -D $1
    done