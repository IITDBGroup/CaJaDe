#!/bin/bash

# mimic
# cp db.conf.mimic db.conf
#python2 explain_LL.py jg_1_et 20 16 |& tee outfile_mimic_1
#python2 explain_LL.py jg_10_et 20 16 |& tee outfile_mimic_10

# nba
cp db.conf.nba db.conf
#python2 explain_LL.py jg_288_et 20 ${num} |& tee outfile_nba_${num}

for num in 16 64 256 512
do
	python2 explain_LL.py jg_288_et 20 ${num} |& tee /experiment_results/exoutfile_nba_${num}
done
