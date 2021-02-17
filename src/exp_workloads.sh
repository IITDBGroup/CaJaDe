#!/bin/bash

# different query workloads

# echo "python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev -t s -m 0 -i false -F 0.3 -D feb12_workloads -W true"

# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev -t s -m 0 -i false -F 0.3 -D feb12_workloads -W true


echo "python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -i false -F 0.3 -D feb12_workloads -W true"

python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -i false -F 0.3 -D feb12_workloads -W true
