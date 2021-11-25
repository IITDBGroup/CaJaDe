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
        logger.debug(graph.edges.data()) ####

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
    logger.debug(tdArr)

    tmp1 = []
    tmp2 = []
    frac_tmp = [] #@@
    # logger.debug(query)


    for i in range(0, rangelen):
      if i<colNum:
          tmp1.append(tdArr[i])
      else:
          tmp2.append(tdArr[i])
    frac_tmp.append(tmp1[0])#@@
    frac_tmp.append(tmp2[0])#@@
    
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


    global ur1
    global ur2 
    
    ur1="".join(tmp1)
    ur2="".join(tmp2)


    for i in range(len(colData)):
        if(ddict[colData[i]]=='string'):
            tmp1[i] = f"'{tmp1[i]}'"
            tmp2[i] = f"'{tmp2[i]}'"

    global u1
    global u2

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
    global resultSchemaName 
    resultSchemaName = datetime.today().strftime('%B%d%H%M')

    run_experiment(conn=globals()['conn'],
                    result_schema=resultSchemaName, #'oct11',
                    user_query=(uQuery, 'test'),
                    user_questions = [u1,u2],
                    user_questions_map = {'yes': ur1 , 'no': ur2},
                    user_specified_attrs=list(set(user_specified_attrs)),
                    user_name=db_user,
                    password=db_pswd,
                    host=db_host,
                    port=db_port,
                    dbname=db_name, 
                    maximum_edges=1,
                    f1_sample_rate=0.3,
                    f1_calculation_type = 'o',
                    user_assigned_max_num_pred=2,
                    min_recall_threshold=0.5,
                    gui=True)

    #query2 = "select p_desc from oct11.global_results"
    query2 = "select id, jg_name, p_desc, is_user, recall, precision from "+resultSchemaName+".global_results" #"select p_desc from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query2)
    exp_list = globals()['cursor'].fetchall()
    print('exp_list:::', exp_list)
    #exp_list = exp_replace_name(exp_list_tmp)
    highlight_list = getHighlightTexts(exp_list)

    #query3 = "select distinct jg_details from oct11.global_results"
    query3 = "select distinct jg_name, jg_details from "+resultSchemaName+".global_results" #"select distinct jg_details from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query3)
    jg_detail_list = globals()['cursor'].fetchall()
    jg = getJoinGraph(jg_detail_list)
    global nodesNameList
    #nodesNameList = []
    #print('nodesNameList:::::::',nodesNameList)
    #print(jg)

    query4 = "select fscore from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query4)
    fscore_list = globals()['cursor'].fetchall()

    query5 = "select jg_details, fscore, p_desc, jg_name from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query5)
    test_list = globals()['cursor'].fetchall()

    query6 = "select jg_name, p_desc from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query6)
    temp = globals()['cursor'].fetchall()
    # print("**********************temp:")
    # print(temp)
    # print("**********************temp[0]")
    # print(temp[0])
    # print("**********************temp[1]")
    # print(temp[1])
    # print("**********************temp[0][0]")
    # print(temp[0][0])
    # print("**********************temp[0][1]")
    # print(temp[0][1])
    # print("**********************")

    # query7 = "select distinct jg_name, jg_details from "+resultSchemaName+".global_results"
    # globals()['cursor'].execute(query7)
    # test_tmp = globals()['cursor'].fetchall()


    # query4 = "select distinct recall from "+resultSchemaName+".global_results"
    # globals()['cursor'].execute(query4)
    # recall_list = globals()['cursor'].fetchall()


    query_u1_frac = f" SELECT COUNT(*) FROM pt_full WHERE {u1};"
    query_u2_frac = f" SELECT COUNT(*) FROM pt_full WHERE {u2};"
    globals()['cursor'].execute(query_u1_frac)
    frac1 = globals()['cursor'].fetchall()
    globals()['cursor'].execute(query_u2_frac)
    frac2 = globals()['cursor'].fetchall()

    fracnames=[ur1, ur2]
    fracvalues=[frac1, frac2] 
    
    return jsonify(result = "success-explanation", result2 = exp_list, result3 = jg, result4 = fscore_list, result5 = test_list, result6 = highlight_list, result7 = nodesNameList, result8=fracnames, result9=fracvalues)
##@@
@app.route('/ratingUD',methods=['UD'])
def ratingUD():
    data = request.get_json()

    likedList = data["likedList"] 
    logger.debug(f"likedList: {likedList}") 
    dislikedList = data["dislikedList"]
    logger.debug(f"dislikedList: {dislikedList}")  
    exp_data_jgname = data["exp_data_jgname"]  

    exp_from_jg_based_on_userfeedback = []
    liked_sims = []
    disliked_sims = []
    li = 0
    di = 0
    for l in likedList:
        li+=1
        l_sim = f"similarity(p_desc::text, '{l}'::text) AS s{li}"
        liked_sims.append(l_sim)
    for d in dislikedList:
        di+=1
        d_sim = f"-similarity(p_desc::text, '{d}'::text) AS d{di}"
        disliked_sims.append(d_sim)

    logger.debug(f"li: {li}")
    logger.debug(f"di: {di}")
    liked_alias=None
    disliked_alias=None

    if(li!=0):
        liked_alias=[f's{la}' for la in range(1, li+1)]
    if(di!=0):
        disliked_alias=[f'd{da}' for da in range(1, di+1)]
    user_alias = liked_alias+disliked_alias
    logger.debug(f"user_alias: {user_alias}")

    drop_updated_view = f"DROP MATERIALIZED VIEW IF EXISTS user_updated_exp;"

    updated_q_view = f"""
    CREATE MATERIALIZED VIEW user_updated_exp AS
    (
    WITH update_by_user AS 
    (
    SELECT id, jg_name, jg_details, p_desc, is_user, recall, precision, fscore,
    {",".join(liked_sims)}, {", ".join(disliked_sims)}
    FROM {resultSchemaName}.patterns
    )
    SELECT id, jg_name, jg_details, p_desc, is_user, recall, precision, fscore
    FROM update_by_user
    ORDER BY ({" + ".join(user_alias)})*fscore::NUMERIC DESC LIMIT 20
    );
    """
    globals()['cursor'].execute(drop_updated_view)
    globals()['cursor'].execute(updated_q_view)

    #query2 = "select p_desc from oct11.global_results"
    query2 = "select id, jg_name, p_desc, is_user, recall, precision from user_updated_exp" #"select p_desc from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query2)
    exp_list = globals()['cursor'].fetchall()
    print('exp_list:::', exp_list)
    #exp_list = exp_replace_name(exp_list_tmp)
    highlight_list = getHighlightTexts(exp_list)

    #query3 = "select distinct jg_details from oct11.global_results"
    query3 = "select distinct jg_name, jg_details from user_updated_exp" #"select distinct jg_details from "+resultSchemaName+".global_results"
    globals()['cursor'].execute(query3)
    jg_detail_list = globals()['cursor'].fetchall()
    jg = getJoinGraph(jg_detail_list)
    global nodesNameList
    #nodesNameList = []
    #print('nodesNameList:::::::',nodesNameList)
    #print(jg)

    query4 = "select fscore from user_updated_exp"
    globals()['cursor'].execute(query4)
    fscore_list = globals()['cursor'].fetchall()

    query5 = "select jg_details, fscore, p_desc, jg_name from user_updated_exp"
    globals()['cursor'].execute(query5)
    test_list = globals()['cursor'].fetchall()

    query6 = "select jg_name, p_desc from user_updated_exp"
    globals()['cursor'].execute(query6)
    temp = globals()['cursor'].fetchall()
    # print("**********************temp:")
    # print(temp)
    # print("**********************temp[0]")
    # print(temp[0])
    # print("**********************temp[1]")
    # print(temp[1])
    # print("**********************temp[0][0]")
    # print(temp[0][0])
    # print("**********************temp[0][1]")
    # print(temp[0][1])
    # print("**********************")

    # query7 = "select distinct jg_name, jg_details from "+resultSchemaName+".global_results"
    # globals()['cursor'].execute(query7)
    # test_tmp = globals()['cursor'].fetchall()


    # query4 = "select distinct recall from "+resultSchemaName+".global_results"
    # globals()['cursor'].execute(query4)
    # recall_list = globals()['cursor'].fetchall()


    query_u1_frac = f" SELECT COUNT(*) FROM pt_full WHERE {u1};"
    query_u2_frac = f" SELECT COUNT(*) FROM pt_full WHERE {u2};"
    globals()['cursor'].execute(query_u1_frac)
    frac1 = globals()['cursor'].fetchall()
    globals()['cursor'].execute(query_u2_frac)
    frac2 = globals()['cursor'].fetchall()

    fracnames=[ur1, ur2]
    fracvalues=[frac1, frac2] 

    return jsonify(result = "success-explanation", result2 = exp_list, result3 = jg, result4 = fscore_list, result5 = test_list, result6 = highlight_list, result7 = nodesNameList, result8=fracnames, result9=fracvalues)


def getHighlightTexts(exp_list):
    print("<<<<<<<exp_list[0]: ", exp_list[0])
    print("<<<<<<<exp_list[0][2]: ", exp_list[0][2])
    highlightTxtList = []
    for i in range(0, len(exp_list)):
        tmp = exp_list[i][2]
        split_comma = tmp.split(',')
        for j in range(0, len(split_comma)):
            tmp_line = split_comma[j]
            get_text = tmp_line.split('.')[0]
            if get_text not in highlightTxtList:
                highlightTxtList.append(get_text)
    print('highlight text list: ', highlightTxtList)
    return highlightTxtList
    ##################################
    # highlightTxtList = []
    # for i in range(0, len(exp_list)):
    #     tmp = str(exp_list[i])
    #     ##print('tmp: ', tmp) ###
    #     tmp = tmp.replace('(', '')
    #     tmp = tmp.replace(')', '')
    #     tmp = tmp.replace('\'', '')
    #     ##print('tmp: ', tmp) ###
    #     split_comma = tmp.split(',')
    #     for j in range(0, len(split_comma)-1):
    #         tmp_line = split_comma[j]
    #         get_text = tmp_line.split('.')[0]
    #         #get_text = get_text.split('_')[0]
    #         if get_text not in highlightTxtList:
    #             highlightTxtList.append(get_text)
    # print('highlight text list: ', highlightTxtList)
    # return highlightTxtList

def getJoinGraph(jg_detail_list):
    gd_list = []
    graphData = {"nodes":[], "links":[]}
    global nodesNameList
    nodesNameList = []
    for i in range (0, len(jg_detail_list)):
        graphData = {"nodes":[], "links":[]} ##
        node_list = []

        cur_data = jg_detail_list[i]
        cur_jg_name = cur_data[0]
        cur_jg_detail = cur_data[1]
        print("cur_jg_detail::::::")
        print(cur_jg_detail)

        if 'cond' not in cur_jg_detail:
            nodeName = 'PT'
            node_list.append(nodeName)
            graphData['nodes'].append({"name": nodeName, "id": cur_jg_name}) #"PT"
            graphData['links'].clear()
        else:
            if '|' not in cur_jg_detail:
                getNodes = cur_jg_detail.split('cond')
                nodes = getNodes[0].split(',')
                two_nodes = []
                for i in range(0, len(nodes)-1):
                    x = nodes[i].split(' ')
                    nodeName = x[len(x)-1]

                    if nodeName not in node_list:
                        node_list.append(nodeName)
                        graphData['nodes'].append({"name": nodeName, "id": cur_jg_name})
                    two_nodes.append(nodeName)
                graphData['links'].append({"source": two_nodes[0], "target": two_nodes[1], "cond": getJGcondition(getNodes[1],node_list)})
            else:
                getNodes = cur_jg_detail.split('|')
                #jgID = getJGid(getNodes[0])
                for i in range(0, len(getNodes)):
                    nodes = getNodes[i].split(',')
                    two_nodes = []
                    for j in range(0, len(nodes)):
                        if 'cond' not in nodes[j]:
                            x = nodes[j].split(' ')
                            nodeName = x[len(x)-1]
                            if nodeName not in node_list:
                                node_list.append(nodeName)
                                graphData['nodes'].append({"name": nodeName, "id": cur_jg_name})
                        
                            two_nodes.append(nodeName)
                        else:
                            jg_condition = nodes[j]
                    graphData['links'].append({"source": two_nodes[0], "target": two_nodes[1], "cond": getJGcondition(jg_condition, node_list) })
        gd_list.append(graphData)
        for node in node_list:
            if node not in nodesNameList:
                nodesNameList.append(node)
    return gd_list
    #################################
    # gd_list = [] ##
    # graphData = {"nodes":[], "links":[]}
    # global nodesNameList
    # nodesNameList = []
    # list_length = len(jg_detail_list)

    # for i in range (0, list_length): ##
    #     graphData = {"nodes":[], "links":[]} ##
    #     node_list = []
    #     jg_tmp = str(jg_detail_list[i]) #test for the first jg #str(jg_detail_list[0])
    #     jg_tmp = jg_tmp.split('\'')[1] 
    #     # print("jg_tmp:")
    #     # print(jg_tmp)

    #     if 'cond' not in jg_tmp:
    #         nodeName = 'PT'
    #         # print("nodeName:")
    #         # print(nodeName)
    #         node_list.append(nodeName) #'PT'
    #         graphData['nodes'].append({"name": nodeName, "id": getJGid(jg_tmp)}) #"PT"
    #         graphData['links'].clear()

    #     else:
    #         if '|' not in jg_tmp:
    #             getNodes = jg_tmp.split('cond')
    #             nodes = getNodes[0].split(',')
    #             two_nodes = []
    #             for i in range(0, len(nodes)-1):
    #                 x = nodes[i].split(' ')
    #                 nodeName = x[len(x)-1]

    #                 if nodeName not in node_list:
    #                     node_list.append(nodeName)
    #                     graphData['nodes'].append({"name": nodeName, "id": getJGid(getNodes[0])})
    #                 two_nodes.append(nodeName)
    #             #print('@@@@node_list: ', node_list)
    #             graphData['links'].append({"source": two_nodes[0], "target": two_nodes[1], "cond": getJGcondition(getNodes[1],node_list)})
    #         else:
    #             getNodes = jg_tmp.split('|')
    #             jgID = getJGid(getNodes[0])
    #             for i in range(0, len(getNodes)):
    #                 nodes = getNodes[i].split(',')
    #                 two_nodes = []

    #                 for j in range(0, len(nodes)):
    #                     if 'cond' not in nodes[j]:

    #                         x = nodes[j].split(' ')
    #                         nodeName = x[len(x)-1]

    #                         if nodeName not in node_list:
    #                             node_list.append(nodeName)
    #                             graphData['nodes'].append({"name": nodeName, "id": jgID})
                        
    #                         two_nodes.append(nodeName)
    #                     else:
    #                          jg_condition = nodes[j]          
    #                 #print('!!!!node_list: ', node_list)
    #                 graphData['links'].append({"source": two_nodes[0], "target": two_nodes[1], "cond": getJGcondition(jg_condition, node_list) })
    #     gd_list.append(graphData)
    #     # print('node_list: ', node_list)
    #     for node in node_list:
    #         if node not in nodesNameList:
    #             nodesNameList.append(node)
    #     # print('nodesNameList: ', nodesNameList)
    # return gd_list

def getJGid(tmp_jgID):
    tmp = tmp_jgID.split(' ')
    jgID = tmp[0]
    return jgID

def getJGcondition(tmp_cond, node_list): ################
    tmp = tmp_cond.split(' ')
    jgCondition = tmp[len(tmp)-1]
    index = 1
    for node_name in node_list:
        replace_name = 'A_'+ str(index)
        jgCondition = jgCondition.replace(replace_name, node_name)
        index += 1
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
