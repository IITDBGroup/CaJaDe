from flask import Flask
from flask import render_template, request, jsonify, redirect, url_for, flash
import psycopg2 as pg2
from networkx import MultiGraph
from src.sg_generator import Schema_Graph_Generator
from src.gprom_wrapper import run_command, GProMWrapper
from src.experiments import run_experiment
from flask.logging import default_handler
import logging
import re
from datetime import datetime

logger = logging.getLogger()
logger.addHandler(default_handler)

app = Flask(__name__)


two_cols = re.compile('\w+\.\w+\s{0,}!?(<|>|<=|>=|=|<>|!=)\s{0,}\w+\.\w+\s{0,}')

@app.route("/")
def index():
    return render_template('main.html')

@app.route("/db/", methods =["POST","GET"])
@app.route('/db/<active_table>', methods=['GET'])
def db_connect(active_table='nba'):

    if request.method == "POST":

        form_data = request.form
        # try:
        print(form_data)
        global db_name
        global db_user
        global db_pswd
        global db_port
        global db_host
        db_name = form_data['dbname']
        db_user = form_data['dbusr']
        db_pswd = form_data['dbpswd']
        db_port = form_data['port']
        db_host = form_data['host']
        logger.debug(db_port)
        logger.debug(db_host)
        globals()['conn'] = pg2.connect(database=db_name, 
            user=db_user, 
            password=db_pswd,
            port=db_port,
            host=db_host)
        globals()['conn'].autocommit = True

        globals()['cursor'] = conn.cursor()
        globals()['info'] = "SUCCESS!"

        table_query = '''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_schema, table_name;
        '''
        cursor.execute(table_query)

        db_tables_raw = list(map(
            lambda x: x[0],
            cursor.fetchall()
        ))

        cajade_sys_tables = ['con_info', 'rel_keys']

        globals()['db_tables'] = [t for t in db_tables_raw if t not in cajade_sys_tables]
        globals()['db_schemas'] = {}
        globals()['db_datatype'] = {}

        for tbl in globals()['db_tables']:
            cursor.execute("select * FROM {} LIMIT 1;".format(tbl))
            globals()['db_schemas'][tbl] = [x[0] for x in cursor.description]

            cursor.execute('''
                select column_name, data_type 
                from information_schema.columns 
                where table_name = '{}';
                        '''.format(tbl))
            globals()['db_datatype'][tbl] = cursor.fetchall()

        g = MultiGraph()
        sgg = Schema_Graph_Generator(conn)

        graph, attr_dict = sgg.generate_graph(g)
        logger.debug(graph)
        logger.debug(attr_dict)

        globals()['json_schema'] = convert_to_graph_json(graph.edges.data())

        return render_template(
            "main.html",                
            db_connection_info=info,
            db_tables=db_tables,
            active_table=active_table,
            db_datatype=db_datatype,
            db_schemas=db_schemas,
            schema_graph_data=json_schema)
    else:
        return render_template(
            "main.html",                
            db_connection_info=info,
            db_tables=db_tables,
            active_table=active_table,
            db_datatype=db_datatype,
            db_schemas=db_schemas,
            schema_graph_data=json_schema)

@app.route('/ajax', methods=['POST'])
def ajax():
  data = request.get_json()
  
  global slt 
  global agg 
  global frm 
  global where 
  global grp 
  
  slt = data["slt"]  
  agg = data["agg"]  
  frm = data["frm"]  
  where = data["where"]  
  grp = data["grp"]

  global query


  query = f"""
  SELECT {slt} {", {}".format(agg) if agg!="" else ""} FROM {frm} {"WHERE {}".format(where) if where !="" else ""} {"GROUP BY {}".format(grp) if grp !="" else ""} \
  """
  logger.debug(query)
  cursor.execute(query)
  data_list = cursor.fetchall()
  
  colnames = [desc[0] for desc in cursor.description]
  return jsonify(result = "success", result2 = data_list, result3=colnames)

######
@app.route('/explanation',methods=['EXP'])
def explanation():
    # query data 
    data = request.get_json()

    tdArr = data["tdArr"]
    colNum = data["colNum"]
    colData = data["colData"]
    rangelen = len(tdArr)

    tmp1 = []
    tmp2 = []
    
    # logger.debug(query)


    for i in range(0, rangelen):
      if i<colNum:
          tmp1.append(tdArr[i])
      else:
          tmp2.append(tdArr[i])

    uQuery = "provenance of ("+query+");"

    # construct information needed for user question
    gen_pt_full(uQuery) # create pt_full
    dtype_q = """
    SELECT ic.column_name, ic.data_type  
    FROM information_schema.tables it, information_schema.columns ic 
    WHERE it.table_name = ic.table_name AND it.table_name='pt_full';
    """
    cursor.execute(dtype_q)

    pg_numeric_list = ['smallint','integer','bigint','decimal','numeric',
    'real','double precision','smallserial','serial','bigserial']

    raw_dtypes = [list(x) for x in cursor.fetchall()]

    for d in raw_dtypes:
        if(d[1] in pg_numeric_list):
            d[1] = 'numeric'
        else:
            d[1] = 'string'

    ddict={x[0]:x[1] for x in raw_dtypes} 
    # query result datatypes, we only need to know whether attribute is numeric or not

    for i in range(len(colData)):
        if(ddict[colData[i]]=='string'):
            tmp1[i] = f"'{tmp1[i]}'"
            tmp2[i] = f"'{tmp2[i]}'"

    u1 = ' AND '.join(['='.join(x) for x in list(zip(colData, tmp1))]) 
    u2 = ' AND '.join(['='.join(x) for x in list(zip(colData, tmp2))]) 

    logger.debug(f"u1: {u1}")
    logger.debug(f"u2: {u2}")

    # adding user specified attrs
    # rules: 
    # 1) group by attributes
    # 2) where clause if certain attributes have equal to some constant
    # 3) TBD
    user_specified_attrs = []
    table_mappings = {}
    tables = [x.strip() for x in frm.split(',')]

    for t in tables:
        t_and_a = re.split(r'\s{1,}', t)
        if(len(t_and_a)==1):
            table_mappings[t_and_a[0]] = t_and_a[0]
        else:
            table_mappings[t_and_a[1]] = t_and_a[0]

    # handle where

    ands = re.split("and", where, flags=re.IGNORECASE)

    for a in ands:
        if(not two_cols.search(a)):
            table, attr = [x.strip() for x in re.split(r'(<|>|<=|>=|=|<>|!=)', a)[0].split('.')]
            user_specified_attrs.append((table_mappings[table], attr))

    groups = [x.strip() for x in grp.split(',')]
    for g in groups:
        table, attr = [x.strip() for x in g.split('.')]
        user_specified_attrs.append((table_mappings[table], attr))

    logger.debug(f"user_specified_attrs : {user_specified_attrs}")
    resultSchemaName = datetime.today().strftime('%B%d%H%M')

    run_experiment(conn=globals()['conn'],
                    result_schema=resultSchemaName, #'oct11',
                    user_query=(uQuery, 'test'),
                    user_questions = [u1,u2],
                    user_questions_map = {'yes': u1.replace("'", "''"), 'no': u2.replace("'", "''")},
                    user_specified_attrs=list(set(user_specified_attrs)),
                    maximum_edges=2,
                    f1_sample_rate=0.3,
                    f1_calculation_type = 's',
                    user_assigned_max_num_pred=2,
                    min_recall_threshold=0.5,
                    gui=True)
    # print(uQuery)

    #query2 = "select p_desc from oct11.global_results"
    query2 = "select p_desc from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query2)
    exp_list = globals()['cursor'].fetchall()

    #query3 = "select distinct jg_details from oct11.global_results"
    query3 = "select distinct jg_details from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query3)
    jg_detail_list = globals()['cursor'].fetchall()
    jg = getJoinGraph(jg_detail_list)
    print(jg)

    query4 = "select fscore from "+resultSchemaName+".global_results" #distinct fscore
    globals()['cursor'].execute(query4)
    fscore_list = globals()['cursor'].fetchall()

    query5 = "select jg_details, fscore, p_desc from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query5)
    test_list = globals()['cursor'].fetchall()

    # query4 = "select distinct recall from "+resultSchemaName+".global_results"
    # globals()['cursor'].execute(query4)
    # recall_list = globals()['cursor'].fetchall()

    # query5 = "select distinct precision from "+resultSchemaName+".global_results"
    # globals()['cursor'].execute(query5)
    # precision_list = globals()['cursor'].fetchall()
    
    return jsonify(result = "success-explanation", result2 = exp_list, result3 = jg, result4 = fscore_list, result5 = test_list)
# def getTestList(test_list):
#     for i in range(0, len(test_list)):
#         tmpList = test_list[i][0]
#         jgid = getJGid(tmpList)
        



def getJoinGraph(jg_detail_list):
    gd_list = [] ##
    graphData = {"nodes":[], "links":[]}
    node_list = []
    list_length = len(jg_detail_list)

    for i in range (0, list_length): ##
        graphData = {"nodes":[], "links":[]} ##
        node_list = []
        jg_tmp = str(jg_detail_list[i]) #test for the first jg #str(jg_detail_list[0])
        jg_tmp = jg_tmp.split('\'')[1] 
        print("jg_tmp:")
        print(jg_tmp)

        if 'cond' not in jg_tmp:
            nodeName = 'PT'
            print("nodeName:")
            print(nodeName)
            node_list.append(nodeName) #'PT'
            graphData['nodes'].append({"name": nodeName, "id": getJGid(jg_tmp)}) #"PT"
            graphData['links'].clear()

        else:
            if '|' not in jg_tmp:
                getNodes = jg_tmp.split('cond')
                nodes = getNodes[0].split(',')
                two_nodes = []
                for i in range(0, len(nodes)-1):
                    x = nodes[i].split(' ')
                    nodeName = x[len(x)-1]

                    if nodeName not in node_list:
                        node_list.append(nodeName)
                        graphData['nodes'].append({"name": nodeName, "id": getJGid(getNodes[0])})
                    two_nodes.append(nodeName)
                graphData['links'].append({"source": two_nodes[0], "target": two_nodes[1], "cond": getJGcondition(getNodes[1])})
            else:
                getNodes = jg_tmp.split('|')
                jgID = getJGid(getNodes[0])
                for i in range(0, len(getNodes)):
                    nodes = getNodes[i].split(',')
                    two_nodes = []

                    for j in range(0, len(nodes)):
                        if 'cond' not in nodes[j]:

                            x = nodes[j].split(' ')
                            nodeName = x[len(x)-1]

                            if nodeName not in node_list:
                                node_list.append(nodeName)
                                graphData['nodes'].append({"name": nodeName, "id": jgID})
                        
                            two_nodes.append(nodeName)
                        else:
                             jg_condition = nodes[j]          
                
                    graphData['links'].append({"source": two_nodes[0], "target": two_nodes[1], "cond": getJGcondition(jg_condition) })
        gd_list.append(graphData)
    return gd_list

def getJGid(tmp_jgID):
    tmp = tmp_jgID.split(' ')
    jgID = tmp[0]
    return jgID

def getJGcondition(tmp_cond):
    tmp = tmp_cond.split(' ')
    jgCondition = tmp[len(tmp)-1]
    return jgCondition


def convert_to_graph_json(ll):
    l_json  = {"nodes":[], "links":[]}

    unique_nodes = {}

    lists = [[l[0], l[1]] for l in ll]

    cur_id = 1
    for l in lists:
        for n in l:
            if n not in unique_nodes:
                unique_nodes[n]=cur_id
                cur_id+=1

    for k,v in unique_nodes.items():
        l_json['nodes'].append({"name":k, "label":k, "id": v})

    for l in ll:
        l_json['links'].append({"source" : unique_nodes[l[0]], "target" : unique_nodes[l[1]], "type" : l[2]['condition']})


    return l_json

def gen_pt_full(query):
    gprom_wrapper = GProMWrapper(user= db_user, passwd=db_pswd, host=db_host, 
        port=db_port, db=db_name, frontend='', backend='postgres',)    

    code, output = gprom_wrapper.runQuery(query)
    if(code==0):
        drop_pt_full = "DROP TABLE IF EXISTS pt_full CASCADE;"
        gen_pt_full_query = output.decode("utf-8")
        # logger.debug(f'gen_original_pt_query:\n {gen_original_pt_query}')
        pt_full_table = f"CREATE TABLE pt_full AS {gen_pt_full_query};"
        cursor.execute(drop_pt_full)
        cursor.execute(pt_full_table)
        conn.commit()
    else:
        logger.debug("error when running gprom command")

if __name__ == '__main__':
    app.run(debug=True)
