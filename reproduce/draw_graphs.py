import argparse
import psycopg2
from ndcg_and_workloads_draw import plot_running_time_against_db_offline, plot_running_time_number_jg_bar_df
from scalability_draws import scalability_draw
from lca1 import lca1_draw
from lca2 import lca2_draw
from prepare_csvs import *
import warnings

warnings.filterwarnings('ignore')



if __name__ == '__main__':
	
	parser = argparse.ArgumentParser(description='Gen CSV files needed to draw graphs for CaJaDe')

	parser.add_argument('-G','--graph_name', metavar="\b", type=str, default='scalability', 
	  help='graph(default: %(default)s)')

	parser.add_argument('-H','--db_host', metavar="\b", type=str, default='localhost',
	  help='database host, (default: %(default)s)')

	parser.add_argument('-P','--port', metavar="\b", type=int, default=5432,
	  help='database port, (default: %(default)s)')

	parser.add_argument('-D','--result_schema', metavar="\b", type=str, default="none",
	  help='result_schema the data is stored')

	parser.add_argument('-O','--output_dir', metavar="\b", type=str, default=".",
	  help='output directory (csvs and pdfs), (default: ')

	parser.add_argument('-R', '--repeat_num', metavar='\b', type=int, default='1',
		help='only useful when reproduce workloads')

	requiredNamed = parser.add_argument_group('required named arguments')

	requiredNamed.add_argument('-U','--user_name', metavar="\b", type=str, required=True,
	  help='owner of the database (required)')

	requiredNamed.add_argument('-p','--password', metavar="\b", type=str, required=True,
	  help='password to the database (required)')

	requiredNamed.add_argument('-d','--db_name', metavar="\b", type=str, required=True,
	  help='database name (required)')

	args=parser.parse_args()

	if(args.graph_name=='ndcg'):
		conn = psycopg2.connect(f"host={args.db_host} dbname={args.db_name} user={args.user_name} password={args.password} port={args.port}")
		prep_ndcg_csv(conn=conn, schema=args.result_schema, dataset=args.db_name, outputdir=args.output_dir)
		plot_running_time_against_db_offline(ds_name=args.db_name, col1='runtime', col2='ndcg_score', filename=f'{args.output_dir}/graph_8f_{args.db_name}')

	if(args.graph_name=='scalability'):
		prep_scalability_csv(host=args.db_host, dbname=args.db_name, user=args.user_name, password=args.password, port=args.port, schema=args.result_schema, dataset=args.db_name, outputdir=args.output_dir)
		scalability_draw(ds_name=args.db_name, filename=f'{args.output_dir}/graph_7_{args.db_name}')
	if(args.graph_name=='casestudy'):
		conn = psycopg2.connect(f"host={args.db_host} dbname={args.db_name} user={args.user_name} password={args.password} port={args.port}")
		prep_casestudy_csv(conn=conn, schema=args.result_schema, dbname=args.db_name, outputdir=args.output_dir)

	if(args.graph_name=='workloads'):
		conn = psycopg2.connect(f"host={args.db_host} dbname={args.db_name} user={args.user_name} password={args.password} port={args.port}")
		prep_workloads_csv(conn=conn, repeat_num=args.repeat_num, schema=args.result_schema, dbname=args.db_name, outputdir=args.output_dir)
		plot_running_time_number_jg_bar_df(f'{args.output_dir}/graph_10_{args.db_name}')

	if(args.graph_name=='lca'):
		conn = psycopg2.connect(f"host={args.db_host} dbname={args.db_name} user={args.user_name} password={args.password} port={args.port}")
		prep_lca_csv(conn=conn, schema=args.result_schema, outputdir=args.output_dir)
		lca1_draw(f'{args.output_dir}/graph_8bc')
		lca2_draw(f'{args.output_dir}/graph_8bc')

	if(args.graph_name=='et'):
		conn = psycopg2.connect(f"host={args.db_host} dbname={args.db_name} user={args.user_name} password={args.password} port={args.port}")
		prep_et_csv(conn=conn, schema=args.result_schema, outputdir=args.output_dir)
