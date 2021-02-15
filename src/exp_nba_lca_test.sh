#!/bin/bash
# 15 vs 12: effects of sampling
trap "kill 0" EXIT 

echo "experiments for LCA size effects on time and quality"


# jg : pt - pgs - p (size 2621)

# samples1=(10 20 50 100 200 400 800 1600 2600)

# jgs1=('jg_288')

# for j in ${jgs1[@]}
# 	do
# 		for s in ${samples1[@]}
# 			do
# 			    python3 lca_exp.py -j ${j} -s ${s}  -P 5433 -D test_${j} -U japerev -p jape -d nba_lca
# 			done
# 	done

# python3 lca_exp.py -j ${jgs1[0]}  -P 5433 -D test_${jgs1[0]} -U japerev -p jape -d nba_lca -f True




# jg : pt - player_salary - player (size 66282)

samples2=(10 100 200 400 800 1600 3200 6400 12800 25600)

jgs2=('jg_31')

for j in ${jgs2[@]}
	do
		for s in ${samples2[@]}
			do
			    python3 lca_exp.py -j ${j} -s ${s}  -P 5433 -D new_test_${j} -U japerev -p jape -d nba_lca
			done
	done

python3 lca_exp.py -j ${jgs2[0]}  -P 5433 -D new_test_${jgs2[0]} -U japerev -p jape -d nba_lca -f True



# jg : pt - patients_admission_info - p (size 50797)

# samples3=(10 20 50 100 200 400 800 1600 3200 6400 12800)

# jgs3=('jg_24')

# for j in ${jgs3[@]}
# 	do
# 		for s in ${samples3[@]}
# 			do
# 			    python3 lca_exp.py -j ${j} -s ${s}  -P 5433 -D test_${j} -U japerev -p jape -d mimic_lca
# 			done
# 	done

# python3 lca_exp.py -j ${jgs3[0]}  -P 5433 -D test_${jgs3[0]} -U japerev -p jape -d mimic_lca -f True




# jg : pt - procedures - patients (size 210184)

# samples4=(10 20 50 100 200 400 800 1600 3200 6400 12800 25600)

# jgs4=('jg_9')

# for j in ${jgs4[@]}
# 	do
# 		for s in ${samples4[@]}
# 			do
# 			    python3 lca_exp.py -j ${j} -s ${s}  -P 5433 -D test_${j} -U japerev -p jape -d mimic_lca
# 			done
# 	done

# python3 lca_exp.py -j ${jgs4[0]}  -P 5433 -D test_${jgs4[0]} -U japerev -p jape -d mimic_lca -f True




# jg : pt (size 50797)

# samples5=(10 20 50 100 200 400 800 1600 3200 6400 12800)

# jgs5=('jg_1')

# for j in ${jgs5[@]}
# 	do
# 		for s in ${samples5[@]}
# 			do
# 			    python3 lca_exp.py -j ${j} -s ${s}  -P 5433 -D test_${j} -U japerev -p jape -d mimic_lca
# 			done
# 	done

# python3 lca_exp.py -j ${jgs5[0]}  -P 5433 -D test_${jgs5[0]} -U japerev -p jape -d mimic_lca -f True

# used to generate find examples of LCA - NBA
# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d nba_lca -t s -m 0 -i false -F 0.5 -D feb14_nba_lca


# used to generate find examples of LCA - MIMIC
# python3 experiments.py -M 3 -p jape -U japerev -P 5433 -d mimic_lca -t s -m 0 -i false -F 0.5 -D feb14_mimic_lca