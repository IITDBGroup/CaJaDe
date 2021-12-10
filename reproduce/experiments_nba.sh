#!/bin/bash

OUTPUTDIR=/experiment_results;

# nba sample and recall threshold 8(f)
# trap "kill 0" EXIT 

# echo "experiments for different sample rate and different sample rate in calculating F-score"

# samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
# maxedges=(1 2 3)

# cajadexplain -H 10.5.0.3 -M 1 -p reproduce -U cajade -P 5432 -d nba -t o -i false -F 1 -D nba_sample
# cajadexplain -H 10.5.0.3 -M 2 -p reproduce -U cajade -P 5432 -d nba -t o -i false -F 1 -D nba_sample
# cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d nba -t o -i false -F 1 -D nba_sample

# for s in ${samplerates[@]}
# 	do 
# 		for e in ${maxedges[@]}
# 		do
# 			echo "Running: Figure 8f) Dataset:NBA, F1-sample-rate: ${s}"
# 		    cajadexplain -H 10.5.0.3 -M ${e} -p reproduce -U cajade -P 5432 -d nba -t s -i false -F ${s} -D nba_sample
# 	    done
# 	done

# # draw it
# python3 draw_graphs.py -H 10.5.0.3 -G ndcg -P 5432 -D nba_sample -O ${OUTPUTDIR} -U cajade -p reproduce -d nba

# nba scalability 7(a)
# echo "experiments scability on NBA"
# array=('01' '05' '2' '4' '8')
# rates=(0.1 0.3 0.5 0.7)

# for s in ${array[@]}
# 	do
# 		for r in ${rates[@]}
# 			do
# 	        echo "Running Figure 7) NBA dataset:NBA scale=${s}, sample_rate=${r}"
# 	        cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d nba${s} -t s -i false -F ${r} -D nba_scalability
# 	    done
#     done



# for r in ${rates[@]}
# 	do
# 	echo "Running scability for NBA dataset: scale=1, sample rate=${r}"
#     cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d nba -t s -i false -F ${r} -D nba_scalability
#     done

# draw it
python3 draw_graphs.py -H 10.5.0.3 -G scalability -P 5432 -D nba_scalability -O ${OUTPUTDIR} -U cajade -p reproduce -d nba


# nba lca


# nba worloads


# nba case study