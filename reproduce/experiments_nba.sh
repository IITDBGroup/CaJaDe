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


# nba lca sampling Figure 8 b) and Figure 8 c)
# Figure 8 b)

# Figure 8 c)


jg1sample_sizes = (50, 100, 200, 400, 1600, 2600)
jg2sample_sizes = (50, 100, 200, 400, 800, 1600, 3200, 6400, 12800, 15000)

for s1 in ${jg1sample_sizes[@]}
	do
		echo "Figure 8 b): sample_size=${s1}"
		python3 /CaJaDe/src/lca_exp.py -H 10.5.0.3 -U cajade -d nba_lca -p reproduce -P 5432 -s ${s1} -j jg_288 -D lca_sample
	done

for s2 in ${jg2sample_sizes[@]}
	do
		echo "Figure 8 c): sample_size=${s2}"
		python3 /CaJaDe/src/lca_exp.py -H 10.5.0.3 -U cajade -d nba_lca -p reproduce -P 5432 -s ${s2} -j jg_31 -D lca_sample
	done

python3 draw_graphs.py -H 10.5.0.3 -G lca -P 5432 -D lca -O ${OUTPUTDIR} -U cajade -p reproduce -d nba_lca


APT,result_schema,time,sample_size,sample_rate,num_match,apt_size,num_attrs,is_ref
1,lca_jg_288,0.04,50,0.02,9,2621,2,0
1,lca_jg_288,0.05,100,0.04,6,2621,2,0
1,lca_jg_288,0.07,200,0.08,8,2621,2,0
1,lca_jg_288,0.18,400,0.15,8,2621,2,0
1,lca_jg_288,0.59,800,0.31,9,2621,2,0
1,lca_jg_288,2.09,1600,0.61,10,2621,2,0
1,lca_jg_288,5.61,2600,0.99,10,2621,2,1
2,lca_jg_31,0.1,50,0,1,66282,2,0
2,lca_jg_31,0.1,100,0,1,66282,2,0
2,lca_jg_31,0.12,200,0,1,66282,2,0
2,lca_jg_31,0.23,400,0.01,1,66282,2,0
2,lca_jg_31,0.62,800,0.01,1,66282,2,0
2,lca_jg_31,2.13,1600,0.02,1,66282,2,0
2,lca_jg_31,8.23,3200,0.05,1,66282,2,0
2,lca_jg_31,32.36,6400,0.1,1,66282,2,0
2,lca_jg_31,128.72,12800,0.19,1,66282,2,0
2,lca_jg_31,179.52,15000,0.23,10,66282,2,1

# nba lca


# nba worloads


# nba case study