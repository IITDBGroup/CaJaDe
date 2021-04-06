#!/bin/bash
# 15 vs 12: effects of sampling
trap "kill 0" EXIT 

# echo "experiments for different sample rate and different sample strategy in calculating F-score"

# # originals: 3 differnt max number of egdes
# python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d nba_rev -t o -i false  -D f1_sample_rate_startsize_100
# python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_rev -t o -i false  -D f1_sample_rate_startsize_100
# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t o -i false  -D f1_sample_rate_startsize_100

# samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
# maxedges=(3)

# for s in ${samplerates[@]}
# 	do 
# 		for e in ${maxedges[@]}
# 		do
# 			# echo "python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_rev -t s -i false -F ${s} -D feb11_nba_15_vs_12_sample_rate_eval"
# 		    # python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_rev -t s -i false -F ${s} -D feb17_nba_sample
# 		    python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_rev -t s -i false -F ${s} -D feb17_nba_sample

# 	    done
# 	done


# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t s -i false -F 0.4 -D f1_sample_rate_startsize_100_04

# echo "experiments for different sample rate and different sample strategy in calculating F-score"

# # originals: 3 differnt max number of egdes
# python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d nba_original -t o -i false  -D april_4_sample
# python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_original -t o -i false  -D april_4_sample
# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_original -t o -i false  -D april_4_sample

# samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
# # samplerates=(0.1 0.3 0.5 0.7)
# maxedges=(1 2 3)

# for s in ${samplerates[@]}
# 	do 
# 		for e in ${maxedges[@]}
# 		do
# 			# echo "python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_rev -t s -i false -F ${s} -D feb11_nba_15_vs_12_sample_rate_eval"
# 		    # python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_rev -t s -i false -F ${s} -D feb17_nba_sample
# 		    # python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_rev -t s -i false -F ${s} -D march11_2
# 		    python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_original -t s -i false -F ${s} -D april_4_sample
# 	    done
# 	done


samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
maxedges=(1 2 3)
iters=(1 2 3 4)

for i in ${iters[@]}
	do
		python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d nba_original -t o -i false  -D april_4_repeat_${i}
		python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_original -t o -i false  -D april_4_repeat_${i}
		python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_original -t o -i false  -D april_4_repeat_${i}

		for s in ${samplerates[@]}
			do 
				for e in ${maxedges[@]}
				do
				    python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_original -t s -i false -F ${s} -D april_4_repeat_${i}
			    done
			done
	done