import re
import logging

logger = logging.getLogger(__name__)


extract_pattern = re.compile('([a-zA-Z]+)_([0-9]+)')
re_PROV = re.compile("PROV")

def encode(rel, rel_alias = 'A', attr_alias = 'a', map_dict = None, is_pt= False):
	"""	
	create(if map dict is None) or extend (if map_dict preexists) renaming mapping dict for given relation

	rel_alias: a pattern string user specified for renaming relation, e.g. if relation_alias = 'a',
	then we rename 1st inserted relation to a_1, next table to a_2 ...

	attr_alias: similar to relation alias, this arg is for attribute renaming.

	rel: a dictionary with relation label and attributes
	
	map_dict: dictionary containing relations & attributes user iserted, default None
	"""

	# logger.debug("***rel***")
	# logger.debug(rel)

	renamed_rel_attrs = []

	# logger.debug("~~~map_dict~~~")
	if(map_dict):
		m=None
		renamed_rel_index_list = []
		renamed_attr_index_list = []
		one_key = [m for m in list(map_dict.keys()) if(m!='max_rel_index' and m!='max_attr_index' and m!='dtypes')][0]
		# print(one_key)
		rel_alias = extract_pattern.search(map_dict[one_key]['renamed_rel']).group(1) 
		for k,v in map_dict.items():
			if(k=='max_rel_index' or k=='max_attr_index' or k=='dtypes'):
				continue
			else:
				renamed_rel_index_list.append(int(extract_pattern.search(v['renamed_rel']).group(2)))
			for k_a,v_a in v['columns'].items():
				if(not extract_pattern.search(v_a)):
					continue
				else:
					renamed_attr_index_list.append(int(extract_pattern.search(v_a).group(2)))
			 # override rel_alias

		if "max_rel_index" in map_dict:
			max_rel_index = map_dict['max_rel_index']
		else:
			max_rel_index = max(renamed_rel_index_list)
			map_dict['max_rel_index'] = max_rel_index

		if "max_attr_index" in map_dict:
			max_attr_index = map_dict['max_attr_index']
		else:
			map_dict['max_attr_index'] = max(renamed_attr_index_list)
			max_attr_index = map_dict['max_attr_index']
	else:
		map_dict = {}
		max_rel_index = 1
		map_dict['max_rel_index'] = max_rel_index
		max_attr_index = 0
		map_dict['max_attr_index'] = max_attr_index

	# update max rel index 
	rel['renamed_rel'] = "{}_{}".format(rel_alias,max_rel_index)
	if(is_pt):
		renamed_attr_dict = dict.fromkeys([x.split(':')[0] for x in rel['columns']],None)
	else:
		renamed_attr_dict = dict.fromkeys([x[0] for x in rel['columns']],None)

	# logger.debug("!!!renamed_attr_dict!!!")
	# logger.debug(renamed_attr_dict)

	pg_numeric_list = ['smallint','integer','bigint','decimal','numeric',
	'real','double precision','smallserial','serial','bigserial']

	if('dtypes' not in map_dict):
		map_dict['dtypes'] = {} 
	if(is_pt):
		for n in rel['columns']:
			n_name = n.split(':')[0]
			n_type = n.split(':')[1]
			if(not re_PROV.search(n_name)):
				renamed_attr_dict[n_name] = n_name
				renamed_n = n_name
			else:
				max_attr_index += 1
				renamed_attr_dict[n_name] = '{}_{}'.format(attr_alias,max_attr_index)
				renamed_n = '{}_{}'.format(attr_alias,max_attr_index)

			if(n_type in pg_numeric_list):
				map_dict['dtypes'][renamed_n] = 'ordinal'
			else:
				map_dict['dtypes'][renamed_n] = 'nominal' 
	else:
		for n in rel['columns']:
			n_name = n[0]
			n_type = n[1]
			max_attr_index += 1
			renamed_attr_dict[n_name] = '{}_{}'.format(attr_alias,max_attr_index)
			renamed_n = '{}_{}'.format(attr_alias,max_attr_index)

			if(n_type in pg_numeric_list):
				map_dict['dtypes'][renamed_n] = 'ordinal'
			else:
				map_dict['dtypes'][renamed_n] = 'nominal' 

	rel['columns'] = renamed_attr_dict

	max_rel_index = max_rel_index+1

	map_dict[rel['key']] = rel

	map_dict['max_rel_index'] = max_rel_index

	map_dict['max_attr_index'] = max_attr_index

	return map_dict	    	

