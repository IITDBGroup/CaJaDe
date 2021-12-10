#!/bin/bash

OUTPUTDIR=/experiment_results;

# nba sample and recall threshold 8(f)
trap "kill 0" EXIT 

echo "experiments for different sample rate and different sample rate in calculating F-score"

samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
maxedges=(1 2 3)

cajadexplain -H 10.5.0.3 -M 1 -p reproduce -U cajade -P 5432 -d nba -t o -i false  -D nba_sample
cajadexplain -H 10.5.0.3 -M 2 -p reproduce -U cajade -P 5432 -d nba -t o -i false  -D nba_sample
cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d nba -t o -i false  -D nba_sample

for s in ${samplerates[@]}
	do 
		for e in ${maxedges[@]}
		do
		    cajadexplain -M ${e} -p reproduce -U cajade -P 5432 -d nba -t s -i false -F ${s} -D nba_sample
	    done
	done

# draw it
python draw_graphs.py -H localhost -G ndcg -P 5432 -D nba_sample -O ${OUTPUTDIR} -U cajade -p reproduce -d nba

# nba scalability 7(a)
# echo "experiments scability on NBA"
# array=('01' '05' '2' '4' '8')
# rates=(0.1 0.3 0.5 0.7)

# for s in ${array[@]}
# 	do
# 		for r in ${rates[@]}
# 			do
# 	        echo "Running scability for NBA dataset: scale=${s}, sample rate=${r}"
# 	        python3 experiments.py -M 3 -p reproduce -U cajade -P 5432 -d nba${s} -t s -i false -F ${r} -D nba_scalability
# 	    done
#     done



# for r in ${rates[@]}
# 	do
# 	echo "Running scability for NBA dataset: scale=1, sample rate=${r}"
#     python3 experiments.py -M 3 -p reproduce -U cajade -P 5433 -d nba -t s -i false -F ${r} -D april_6_nba_scalability_2
#     done



#     cajadexplain -M 3 -p nba -U lchenjie -P 5432 -d nba -t s -i false -F 0.3 -D nba_scalability
