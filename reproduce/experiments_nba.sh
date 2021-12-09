#!/bin/bash

# nba sample and recall threshold 8(f)
trap "kill 0" EXIT 

echo "experiments for different sample rate and different sample rate in calculating F-score"

samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
maxedges=(1 2 3)

python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d nba_original -t o -i false  -D nba_sample
python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_original -t o -i false  -D nba_sample
python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_original -t o -i false  -D nba_sample

for s in ${samplerates[@]}
	do 
		for e in ${maxedges[@]}
		do
		    python3 experiments.py -M ${e} -p jape -U japerev -P 5433 -d nba_original -t s -i false -F ${s} -D nba_sample
	    done
	done
# draw it

# nba scalability 7(a)
echo "experiments scability on NBA"
array=('01' '05' '2' '4' '8')
rates=(0.1 0.3 0.5 0.7)

for s in ${array[@]}
	do
		for r in ${rates[@]}
			do
	        echo "Running scability for NBA dataset: scale=${s}, sample rate=${r}"
	        python3 experiments.py -M 3 -p reproduce -U cajade -P 5432 -d nba${s} -t s -i false -F ${r} -D nba_scalability
	    done
    done



for r in ${rates[@]}
	do
	echo "Running scability for NBA dataset: scale=1, sample rate=${r}"
    python3 experiments.py -M 3 -p reproduce -U cajade -P 5433 -d nba -t s -i false -F ${r} -D april_6_nba_scalability_2
    done
