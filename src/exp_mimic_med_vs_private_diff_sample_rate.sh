#!/bin/bash
# medicare vs private: effects of sampling
trap "kill 0" EXIT 

echo "experiments for different sample rate and different sample strategy in calculating F-score"

# python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d mimic_rev -t o -m 0 -i false -F 0.05 -D feb11_med_vs_private_sample_rate_eval
# python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d mimic_rev -t s -m 0 -i false -F 0.05 -D feb11_med_vs_private_sample_rate_eval


array=(0.1 0.2 0.3 0.4 0.5 0.6 0.7)

for s in ${array[@]}
	do
        # echo "Running query of f1 sample rate = ${s}"
        sample=$(bc <<< "${s}*100/1")
        echo "${sample}"
        echo "python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d mimic_rev -t s -m 0 -i false -F ${s} -D feb11_med_vs_private_sample_rate_eval"
	    python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d mimic_rev -t s -m 0 -i false -F ${s} -D feb11_med_vs_private_sample
    done
