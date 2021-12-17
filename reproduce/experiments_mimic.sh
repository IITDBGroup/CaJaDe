#!/bin/bash

OUTPUTDIR=/experiment_results;
question="insurance='Private'|insurance='Medicare'";
query="select a.insurance, 1.0*SUM(hospital_expire_flag)/count(*) as death_rate from admissions a group by a.insurance";

# mimic sample and recall threshold 8(f)
trap "kill 0" EXIT 

##########################################################################################################################
# f1 sample effects on runtime and quality 
echo "experiments for different sample rate and different sample rate in calculating F-score"
samplerates=(0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7)
maxedges=(1 2 3)

cajadexplain -H 10.5.0.3 -M 1 -p reproduce -U cajade -P 5432 -d mimic -t o -i false -F 1 -D mimic_sample -Q ${query} -A ${question}
cajadexplain -H 10.5.0.3 -M 2 -p reproduce -U cajade -P 5432 -d mimic -t o -i false -F 1 -D mimic_sample -Q ${query} -A ${question}
cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d mimic -t o -i false -F 1 -D mimic_sample -Q ${query} -A ${question}

for s in ${samplerates[@]}
	do 
		for e in ${maxedges[@]}
		do
			echo "Running: Figure 8f) Dataset:mimic, Maximum_Edge_Size: ${e}, F1-sample-rate: ${s}"
			echo "cajadexplain -H 10.5.0.3 -M ${e} -p reproduce -U cajade -P 5432 -d mimic -t s -i false -F ${s} -D mimic_sample -Q ${query} -A ${question}"
		    cajadexplain -H 10.5.0.3 -M ${e} -p reproduce -U cajade -P 5432 -d mimic -t s -i false -F ${s} -D mimic_sample -Q ${query} -A ${question}
	    done
	done

# draw it
python3 /CaJaDe/reproduce/draw_graphs.py -H 10.5.0.3 -G ndcg -P 5432 -D mimic_sample -O ${OUTPUTDIR} -U cajade -p reproduce -d mimic

# ###########################################################################################################################

# # mimic scalability 7(b)
echo "experiments scability on mimic"
array=('01' '05' '2' '4' '8')
rates=(0.1 0.3 0.5 0.7)

for s in ${array[@]}
	do
		for r in ${rates[@]}
			do
	        echo "Running Figure 7) mimic dataset:mimic scale=${s}, sample_rate=${r}"
	        cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d mimic${s} -t s -i false -F ${r} -D mimic_scalability
	    done
    done



for r in ${rates[@]}
	do
	echo "Running scability for mimic dataset: scale=1, sample rate=${r}"
    cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d mimic -t s -i false -F ${r} -D mimic_scalability
    done 

# draw it
python3 /CaJaDe/reproduce/draw_graphs.py -H 10.5.0.3 -G scalability -P 5432 -D mimic_scalability -O ${OUTPUTDIR} -U cajade -p reproduce -d mimic

# ###########################################################################################################################
# # mimic lca sampling Figure 8 d) and Figure 8 e)
# # Figure 8 b)
jg1sample_sizes=(50 100 200 400 800 1600 3200 6400 12800 15000)

jg2sample_sizes=(50 100 200 400 800 1600 3200 6400 12800 15000)


for s1 in ${jg1sample_sizes[@]}
	do
		echo "Figure 8 d): sample_size=${s1}"
		python3 /CaJaDe/src/lca_exp.py -H 10.5.0.3 -U cajade -d mimic_lca -p reproduce -P 5432 -s ${s1} -j jg_1 -D jg_1_lca
	done

# Figure 8 c)
for s2 in ${jg2sample_sizes[@]}
	do
		echo "Figure 8 e): sample_size=${s2}"
		python3 /CaJaDe/src/lca_exp.py -H 10.5.0.3 -U cajade -d mimic_lca -p reproduce -P 5432 -s ${s2} -j jg_24 -D jg_24_lca
	done

#draw them
python3 /CaJaDe/reproduce/draw_graphs.py -H 10.5.0.3 -G lca -P 5432 -D lca -O ${OUTPUTDIR} -U cajade -p reproduce -d mimic_lca



##########################################################################################################################
# mimic worloads
echo "Experiment mimic workloads "
iters=(1 2 3)

for i in ${iters[@]}
	do
		cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d mimic -t s -F 0.3 -D mimic_workload${i} -W true
	done

python3 /CaJaDe/reproduce/draw_graphs.py -H 10.5.0.3 -G workloads -P 5432 -D mimic_workload -O ${OUTPUTDIR} -U cajade -p reproduce -d mimic -R 3


##########################################################################################################################
# mimic case study
cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d mimic -t o -m 0 -D casestudy -C true 

python3 /CaJaDe/reproduce/draw_graphs.py -H 10.5.0.3 -G casestudy -P 5432 -D casestudy -O ${OUTPUTDIR} -U cajade -p reproduce -d mimic


#########################################################################################################################

# cajadexplain  -M 1 -p 123 -U japerev -P 5433 -d mimic_original -t s -i false -F 0.3 -D mimic_sample \
# -Q "select a.insurance, 1.0*SUM(hospital_expire_flag)/count(*) as death_rate from admissions a group by a.insurance" \
# -A "insurance='Private'|insurance='Medicare'"