import argparse
import psycopg2
from ndcg_and_workloads_draw import plot_running_time_against_db_offline

nba_scalability_dict = {
'file_name' : 'nba_scalability.csv',
# 'cols' : ['f1_sample_rate','maximum_edges','feature_reduct','lca','materialize_jg','refinment','f1_sample','jg_enumeration','f1_calc'],
'cols': ['f1_sample_rate','maximum_edges','feature_reduct', 'lca', 'materialize_jg', 'refinment', 'f1_sample', 'jg_enumeration', 'run_f1_query']

size,f1_sample_rate,feature_reduct,lca,materialize_jg,refinment,f1_sample,jg_enumeration,f1_calc,total

}


nba_ndcg_runtime_dict = {
'file_name': 'ndcg_nba.csv'
}

# def main(graph_name, info_dict)


if __name__ == '__main__':
	
	parser = argparse.ArgumentParser(description='Gen CSV files needed to draw graphs for CaJaDe')

	parser.add_argument('-G','--graph name', metavar="\b", type=str, default='scalability', 
	  help='graph(default: %(default)s)')

	parser.add_argument('-D','--dataset', metavar="\b", type=str, default="nba",
	  help='dataset : nba/mimic (default: %(default)s)')

	parser.add_argument('-H','--db_host', metavar="\b", type=str, default='localhost',
	  help='database host, (default: %(default)s)')

	parser.add_argument('-P','--port', metavar="\b", type=int, default=5432,
	  help='database port, (default: %(default)s)')

	parser.add_argument('-D','--result_schema', metavar="\b", type=str, default="none",
	  help='result_schema the data is stored')

	parser.add_argument('-O','--output_dir', metavar="\b", type=str, default=".",
	  help='output directory (csvs and pdfs), (default: ')

	requiredNamed.add_argument('-U','--user_name', metavar="\b", type=str, required=True,
	  help='owner of the database (required)')

	requiredNamed.add_argument('-p','--password', metavar="\b", type=str, required=True,
	  help='password to the database (required)')

	requiredNamed.add_argument('-d','--db_name', metavar="\b", type=str, required=True,
	  help='database name (required)')

	args=parser.parse_args()

	conn = psycopg2.connect(f"host={args.db_host} dbname={args.db_name} user={args.user_name} password={args.password} port={args.port}")
	if(args.graph_name=='ndcg'):
		prep_ndcg_csv(conn=conn, dataset=args.db_name, schema=args.result_schema, outputdir=args.output_dir)
		plot_running_time_against_db_offline(ds_name=args.db_name, col1='runtime', col2='ndcg_score', filename=f'{args.output_dir}/graph_8f_{args.db_name}.csv')
	if(args.graph_name=='scalability'):
		result_csv = prep_scalability_csv(conn=conn, schema=args.result_schema)
	if(args.graph_name=='casestudy'):
		result_csv = prep_case_study_csv(conn=conn, schema=args.result_schema)
	if(args.graph_name=='workloads'):
		result_csv = prep_workloads_csv(conn=conn, schema=args.result_schema)
	if(args.graph_name=='lca'):
		result_csv = prep_lca_csv(conn=conn, schema=args.result_schema)

