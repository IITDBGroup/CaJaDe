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
# python3 draw_graphs.py -H 10.5.0.3 -G scalability -P 5432 -D nba_scalability -O ${OUTPUTDIR} -U cajade -p reproduce -d nba


# nba lca sampling Figure 8 b) and Figure 8 c)
# Figure 8 b)

# Figure 8 c)



# jg1sample_sizes = (50 100 200 400 1600 2600)
# jg2sample_sizes = (50 100 200 400 800 1600 3200 6400 12800 15000)


# for s1 in ${jg1sample_sizes[@]}
# 	do
# 		echo "Figure 8 b): sample_size=${s1}"
# 		python3 /CaJaDe/src/lca_exp.py -H 10.5.0.3 -U cajade -d nba_lca -p reproduce -P 5432 -s ${s1} -j jg_288 -D lca_sample
# 	done

# for s2 in ${jg2sample_sizes[@]}
# 	do
# 		echo "Figure 8 c): sample_size=${s2}"
# 		python3 /CaJaDe/src/lca_exp.py -H 10.5.0.3 -U cajade -d nba_lca -p reproduce -P 5432 -s ${s2} -j jg_31 -D lca_sample
# 	done

# python3 draw_graphs.py -H 10.5.0.3 -G lca -P 5432 -D lca -O ${OUTPUTDIR} -U cajade -p reproduce -d nba_lca




# jg1sample_sizes=(50 100 200 400 1600 2600)
# jg2sample_sizes=(50 100 200 400 800 1600 3200 6400 12800 15000)

# for s1 in ${jg1sample_sizes[@]}
# 	do
# 		echo "Figure 8 b): sample_size=${s1}"
# 		python3 ../src/lca_exp.py -H localhost -U lchenjie -d nba_lca -p reproduce -P 5433 -s ${s1} -j jg_288 -D jg_288_lca
# 	done

# for s2 in ${jg2sample_sizes[@]}
# 	do
# 		echo "Figure 8 c): sample_size=${s2}"
# 		python3 ../src/lca_exp.py -H localhost -U lchenjie -d nba_lca -p reproduce -P 5433 -s ${s2} -j jg_31 -D jg_31_lca
# 	done


python3 draw_graphs.py -H localhost -G lca -P 5433 -D lca -O . -U lchenjie -p reproduce -d nba_lca


#   -h, --help            show this help message and exit
#   -G, --graph_name  graph(default: scalability)
#   -H, --db_host     database host, (default: localhost)
#   -P, --port        database port, (default: 5432)
#   -D, --result_schema 
#                         result_schema the data is stored
#   -O, --output_dir  output directory (csvs and pdfs), (default:

# required named arguments:
#   -U, --user_name   owner of the database (required)
#   -p, --password    password to the database (required)
#   -d, --db_name     database name (required)



# nba lca


# nba worloads


# nba case study