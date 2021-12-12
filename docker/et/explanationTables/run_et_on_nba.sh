#!/bin/bash


for num in 16 64 256 512
do
	python2 explain_LL.py jg_288_et 20 ${num} |& tee /experiment_results/explanation_table_sample_size_${num}_runtime
done
