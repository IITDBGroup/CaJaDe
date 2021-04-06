#!/bin/bash
# medicare vs private: effects of sampling
trap "kill 0" EXIT 

echo "experiments scability on MIMIC"

array=('01' '05' '2' '4' '8')

rates=(0.1 0.3 0.5 0.7)

for s in ${array[@]}
	do
		for r in ${rates[@]}
			do
        # echo "Running query of f1 sample rate = ${s}"
	        # python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev_${s} -t s -m 0 -i false -F ${r} -D march_3_mimic_scalability
	        # python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev_${s} -t s -m 0 -i false -F ${r} -D april_2_mimic_scalability
	        python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev_${s} -t s -m 0 -i false -F ${r} -D april_4_mimic_scalability
	    done
    done



for r in ${rates[@]}
	do
	# echo "Running query of f1 sample rate = ${s}"
    # python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_rev -t s -m 0 -i false -F ${r} -D march_3_mimic_scalability
    # python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_original -t s -m 0 -i false -F ${r} -D april_2_mimic_scalability
    python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_original -t s -m 0 -i false -F ${r} -D april_4_mimic_scalability
    done
