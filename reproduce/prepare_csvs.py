from sklearn.metrics import ndcg_score
import psycopg2
import pandas as pd 
import numpy as np
import argparse


# true_relevance = np.asarray([[10, 0, 0, 1, 5]])
# we predict some scores (relevance) for the answers
# scores = np.asarray([[.1, .2, .3, 4, 70]])



# give sample rate and max edge size

# schema : f1_sample_rate_startsize_100
# max_edge : 3
# sample_rate : 0.05


def prep_workloads_csv(conn, schema, outputdir):
	pass

def prep_case_study_csv(conn, schema, outputdir):
	pass

def prep_scalability_csv(host, dbname, user, password, port, schema, dataset, outputdir):
	db_scales = ['01', '05', '1', '2', '4', '8']
	df = pd.DataFrame(columns=['size','f1_sample_rate','feature_reduct','lca','materialize_jg','refinment','f1_sample','jg_enumeration','f1_calc','total'])
	for s in db_scales:
		if(s=='01'):
			size=0.1
		elif(s=='05'):
			size=0.5
		elif(s=='1'):
			s=''
			size=1
		else:
			size=s
		conn = psycopg2.connect(host=host, dbname=f"{dataset}{s}", user=user, password=password, port=port)
		q=f"""
			SELECT {size} AS size, f1_sample_rate, feature_reduct, lca, materialize_jg, refinment, f1_sample, jg_enumeration, run_f1_query::numeric+check_recall::numeric AS f1_calc,
			feature_reduct::NUMERIC +lca::NUMERIC +materialize_jg::NUMERIC +refinment::NUMERIC +f1_sample::NUMERIC +jg_enumeration::NUMERIC +run_f1_query::NUMERIC + check_recall::NUMERIC AS total
			FROM {dataset}{s}.{schema}.time_and_params
			"""
		s_df=pd.read_sql(q, conn)
		conn.close()
		df = df.append(s_df)

	print("prepared_df:")
	print(df)

	df.to_csv(f'{outputdir}/graph_7_{dataset}.csv', index=False)


def prep_ndcg_csv(conn, schema, dataset, outputdir):

	df_ret = pd.DataFrame(columns=['edge','sample_rate', 'runtime', 'ndcg_score'])
	cur  = conn.cursor()

	sample_rates = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7]
	edges = [1,2,3]

	for e in edges:
		gt_query = f"""SELECT count(*) as gt_cnt
		FROM {schema}.time_and_params t, {schema}.global_results p
		WHERE t.exp_desc = p.exp_desc AND t.maximum_edges = '{e}'
		AND t.f1_calculation_type = 'o'"""
		df_gt_count = pd.read_sql(gt_query, conn)
		gt_cnt = df_gt_count['gt_cnt'].to_list()[0]

		for s in sample_rates:
			fetch_q = f"""
			WITH sample_result AS 
			(
				SELECT p.is_user, p.jg, p.p_desc, p.fscore, t.maximum_edges, 
				t.total, t.f1_sample_rate
				FROM {schema}.time_and_params t, {schema}.global_results p
				WHERE t.exp_desc = p.exp_desc AND t.maximum_edges = '{e}'
				AND t.f1_sample_rate = '{s}'
			),
			ground_truth AS 
			(
				SELECT p.is_user, p.jg, p.p_desc, p.fscore, t.maximum_edges
				FROM {schema}.time_and_params t, {schema}.global_results p 
				WHERE t.exp_desc = p.exp_desc AND t.maximum_edges = '{e}'
				AND t.f1_calculation_type = 'o'
			),
			oj as
			(
			SELECT
				g.fscore::numeric as t_f1,
				s.fscore::numeric as s_f1
				FROM sample_result s
				FULL OUTER JOIN ground_truth g
				ON g.is_user=s.is_user AND 
				g.jg=s.jg AND g.p_desc = s.p_desc 
			)
			(

				SELECT
				1 AS tf,
				row_number() over () as id, 
				t_f1,
				CASE WHEN s_f1 is null then '0' else s_f1 end as s_f1
				from oj 
				WHERE t_f1 is not null
			)
			UNION 
			(
				SELECT 
				2 AS tf,
				row_number() over () as id, 
				'0' AS t_f1,
				s_f1
				from oj 
				WHERE t_f1 is null and s_f1 is not null
			)
			order by tf, id
			"""
			
			row_dict = {}

			dfts = pd.read_sql(fetch_q, conn)
			out = np.empty(dfts.shape[0], dtype=object)
			tlist = dfts['t_f1'].to_list()
			# print(tlist)
			slist = dfts['s_f1'].to_list()
			# print(slist)
			tarray = np.asarray([tlist])
			sarray = np.asarray([slist])
			score = ndcg_score(tarray, sarray) # ndcg
			# score = len(list(filter(lambda x: x > 0, slist[:gt_cnt]))) / gt_cnt # recall
			row_dict['ndcg_score'] = score

			param_q = f"""
			SELECT round(total::numeric,2) as runtime
			FROM {schema}.time_and_params
			where f1_calculation_type = 's'
			AND f1_sample_rate = '{s}' and maximum_edges='{e}'
			"""
			
			cur.execute(param_q)

			row_dict['edge'] = e
			row_dict['sample_rate'] = s
			row_dict['runtime'] = cur.fetchone()[0]

			df_ret = df_ret.append(row_dict, ignore_index=True)				

	tot_q = f"""
	SELECT maximum_edges as edge,
	f1_sample_rate as sample_rate, 
	round(total::numeric,2) as runtime,
	'1' as ndcg_score
	FROM {schema}.time_and_params
	where f1_calculation_type = 'o'
	"""

	df_tot = pd.read_sql(tot_q, conn)

	df_ret = pd.concat([df_ret, df_tot])

	df_ret.to_csv(f'{outputdir}/graph_8f_{dataset}.csv', index=False)

	return df_ret


if __name__ == '__main__':

	parser = argparse.ArgumentParser(description='reproduce graph 8(f) of CaJaDE paper')

	parser.add_argument('-H','--db_host', metavar="\b", type=str, default='localhost',
	  help='database host, (default: %(default)s)')

	parser.add_argument('-P','--port', metavar="\b", type=int, default=5432,
	  help='database port, (default: %(default)s)')

	parser.add_argument('-D','--result_schema', metavar="\b", type=str, default="none",
	  help='result_schema_name_prefix, (default: exp_[timestamp of the start]')

	requiredNamed.add_argument('-U','--user_name', metavar="\b", type=str, required=True,
	  help='owner of the database (required)')

	requiredNamed.add_argument('-p','--password', metavar="\b", type=str, required=True,
	  help='password to the database (required)')

	requiredNamed.add_argument('-d','--db_name', metavar="\b", type=str, required=True,
	  help='database name (required)')

	# %%%%%%%%%%%%%%%%%%%%%%%%%%% MIMIC %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

	# conn = psycopg2.connect(f"dbname=mimic_original user=japerev port=5433")
	# result = calc_ndcg(conn=conn, schema='april_4_sample')

	# %%%%%%%%%%%%%%%%%%%%%%%%%%% NBA %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

	conn = psycopg2.connect(f"host={args.db_host} dbname={agrs.db_name} user={args.user_name} password={args.password} port={args.port}")
	result = prep_ndcg_csv(conn=conn, schema=args.result_schema)
