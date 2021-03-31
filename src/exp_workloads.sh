#!/bin/bash

# different query workloads


# %%%%%%%%%%%%%%%%%%%%%%NBA %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# echo "python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -F 0.3 -D feb12_workloads -W true"

# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -F 0.3 -D feb12_workloads -W true

# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false -D march18_no_sample_no_salary -W true

# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_original -t o -m 0 -D march23 -W true

# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_original -t o -i false -m 0 -D march23 -W true


# %%%%%%%%%%%%%%%%%%%%%%%%5MIMIC%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# echo "python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev -t s -m 0  -F 0.3 -D feb12_workloads_new -W true"

# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev -t o -m 0 -i false -D march18_no_sample_new_chapter -W true

# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_original -t o -m 0 -D march22 -W true

python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_original -t o -m 0 -D march24 -W true

