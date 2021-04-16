#!/bin/bash
# 15 vs 12: effects of sampling
trap "kill 0" EXIT 

# select f1_sample_rate, feature_reduct, lca, run_f1_query::numeric+check_recall::numeric as f1_calculation, materialize_jg, refinment, f1_sample, jg_enumeration, rank_patterns, total from march_3_nba_naive.time_and_params;


# nba
# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -o n -t o -m 0 -i false -F 1 -D march_3_nba_naive
# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_original -o n -t o -i false -F 1 -D april_14_nba_naive

# mimic
# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev -o n -t o -m 0 -i false -F 1 -D march_3_mimic_naive
python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_original -o n -t o -i false -F 1 -D april_14_mimic_naive
