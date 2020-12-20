#!/bin/bash
trap "kill 0" EXIT 

echo "experiments for different db size in calculating F-score"

echo "schema desc=$1"

dbnames="nba01 nba05 nba nba2 nba5 nba10 "

for d in $dbnames:
do
    python3 experiments.py -M 3 -p lcj53242 -U lchenjie -P 5433 -d $d -t s -F 0.7 -r 0.05 -f 0.5 -D $1
done
