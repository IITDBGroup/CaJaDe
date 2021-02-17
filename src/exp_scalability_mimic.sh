#!/bin/bash
# medicare vs private: effects of sampling
trap "kill 0" EXIT 

echo "experiments scability on MIMIC"

array=('01' '05' '2' '4' '8')

# 2 edge max
# for s in ${array[@]}
# 	do
#         # echo "Running query of f1 sample rate = ${s}"
#         echo "python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d mimic_rev_${s} -t s -m 0 -i false -F 0.1 -D feb11_med_vs_private_scability"
#         python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d mimic_rev_${s} -t s -m 0 -i false -F 0.1 -D feb11_med_vs_private_scability
#     done



3 edge max
python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev -t s -m 0 -i false -F 0.1 -D feb16_med_vs_private_scability


for s in ${array[@]}
	do
        # echo "Running query of f1 sample rate = ${s}"
        python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev_${s} -t s -m 0 -i false -F 0.1 -D feb16_med_vs_private_scability
    done