#!/bin/bash
# medicare vs private: effects of sampling
trap "kill 0" EXIT 

echo "experiments for different sample rate and different sample strategy in calculating F-score"

# originals: 3 differnt max number of egdes
# python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d mimic_original -t o -m 0 -i false  -D april_4_sample
# python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d mimic_original -t o -m 0 -i false  -D april_4_sample
# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_original -t o -m 0 -i false  -D april_4_sample

samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
maxedges=(1 2 3)
iters=(1 2 3 4 5)

for i in ${iters[@]}
	do
		python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d mimic_original -t o -i false  -D april_15_repeat_${i}
		python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d mimic_original -t o -i false  -D april_15_repeat_${i}
		python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_original -t o -i false  -D april_15_repeat_${i}

		for s in ${samplerates[@]}
			do 
				for e in ${maxedges[@]}
				do
				    python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d mimic_original -t s -i false -F ${s} -D april_15_repeat_${i}
			    done
			done
	done



# samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
# maxedges=(4)

# for s in ${samplerates[@]}
# 	do 
# 		for e in ${maxedges[@]}
# 		do
# 		    python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d mimic_rev -t s -F ${s} -D f1_sample_rate_4_with_exclude_expensive
# 	    done
# 	done


# python3 experiments.py -M 4 -p jape -U japerev -P 5433 -d mimic_rev -t s -F 0.05 -D f1_sample_rate_4_with_exclude_expensive
