import pandas as pd 
import re

df = pd.read_csv('NBA_case_Q5.csv')

def extract_edges(raw_jg):
	raw_list = re.findall('\(.*?\=.*?\)', raw_jg)
	raw_list = [re.sub("(\_)(?=[^0-9])", r'\\_', x) for x in raw_list]
	raw_list = [re.sub("\w+?\_([0-9])",r'$A_\1$', x) for x in raw_list]
	if(not raw_list):
		return "\makecell{A\_1}"
	x = '\\\\'.join(raw_list)
	y = "{"+x+"}"
	return f"\makecell{y}".replace('"','')

def extract_nodes(raw_simple_jg):
	raw_list = re.findall("[0-9]: \w+", raw_simple_jg)
	raw_list = [re.sub("([0-9])",r'$A_\1$', x) for x in raw_list]
	raw_list = [re.sub("(\_)(?=[^0-9])", r'\\_', x) for x in raw_list]
	x = ' \\\\ '.join(list(dict.fromkeys(raw_list)))
	y = "{"+x+"}"
	return f"\makecell{y}".replace('"','')

def replace_pdesc(pdesc_raw):
	# raw_list = re.findall('(\w+)?_[0-9].', pdesc_raw)
	raw_list = [re.sub("\w+?\_([0-9])",r'$A_\1$', x) for x in pdesc_raw.split(',')]
	raw_list = [re.sub("(\_)(?=[^0-9])", r'\\_', x) for x in raw_list]
	x = ' \\\\ '.join(raw_list)
	y = "{"+x+"}"
	return f"\makecell{y}".replace('"','')

def add_slash(is_user):
	return str(is_user)+"\\\\"
# def remove_dup(strng):
#     return ', '.join(list(dict.fromkeys(strng.split(', '))))


df['edges'] = df['jg_details'].apply(str).apply(extract_edges)
df['nodes'] = df['jg'].apply(str).apply(extract_nodes)
df['precision'] = df['precision'].round(2)
df['recall'] = df['recall'].round(2)
df['fscore'] = df['fscore'].round(2)
df['p_desc'] = df['p_desc'].apply(replace_pdesc)
df['is_user'] = df['is_user'].apply(add_slash)

df = df.rename(columns={"p_desc": "pattern_desc", "is_user": "dir"})
df = df[['nodes','edges','pattern_desc','precision','recall','fscore','dir']]

# df = df.drop(['jg', 'jg_details'], axis=1)
df.to_csv('NBA_Q5_processed.csv', sep='\t', index=False)
