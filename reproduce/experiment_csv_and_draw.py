import argparse

nba_scalability_dict = {
'file_name' : 'nba_scalability.csv'
'cols' : ['f1_sample_rate','maximum_edges','feature_reduct','lca','materialize_jg','refinment','f1_sample','jg_enumeration','f1_calc','total']
}

































if __name__ == '__main__':
	
	parser = argparse.ArgumentParser(description='Gen CSV files needed to draw graphs for CaJaDe')

	parser.add_argument('-G','--graph name', metavar="\b", type=str, default='scalability', 
	  help='graph(default: %(default)s)')

	parser.add_argument('-D','--dataset', metavar="\b", type=str, default="nba",
	  help='dataset: (default: %(default)s)')


	args=parser.parse_args()