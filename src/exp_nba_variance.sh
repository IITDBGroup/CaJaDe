#!/bin/bash
# 15 vs 12: effects of sampling
trap "kill 0" EXIT 

samplerates=(0.1 0.2 0.3 0.4 0.5)

declare -i iterations1=40
declare -i iterations2=30
declare -i iterations3=10

python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false  -D march11_variance
python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false  -D march11_variance
python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t o -m 0 -i false  -D march11_variance

for s in ${samplerates[@]}
	do 
		x=1
		while [ $x -le $iterations1 ]
		do
			python3 experiments.py -M 1 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -i false -F ${s} -D march11_variance
			x=$(( $x+1 ))
		done	
	done

for s in ${samplerates[@]}
	do 
		x=1
		while [ $x -le $iterations2 ]
		do
			python3 experiments.py -M 2 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -i false -F ${s} -D march11_variance
			x=$(( $x+1 ))
		done	
	done

for s in ${samplerates[@]}
	do 
		x=1
		while [ $x -le $iterations3 ]
		do
			python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_rev -t s -m 0 -i false -F ${s} -D march11_variance
			x=$(( $x+1 ))
		done	
	done