from flask import Flask
from flask import render_template, request, jsonify, redirect, url_for, flash
import psycopg2 as pg2
from networkx import MultiGraph
from src.sg_generator import Schema_Graph_Generator
from src.experiments import run_experiment



app = Flask(__name__)


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
  
  slt = data["slt"]
  agg = data["agg"]
  frm = data["frm"]
  grp = data["grp"]
  global query
  
  #[ERROR] missing <select> or <from>
  if slt == "" or frm == "":
      return 'OK'
    
  #select <select> from <from> group by <group>
  if agg == "" and grp != "": 
    query = "select "+slt+" "+" from "+frm+" group by "+grp
      
  #select <select> from <from>
  elif agg == "" and grp == "": 
    query = "select "+slt+" "+" from "+frm
      
  #select <select>, <agg> from <from>
  elif agg != "" and grp == "": 
    query = "select "+slt+", "+agg+" from "+frm
      
  #select <select>, <agg> from <from> group by <group>
  else: 
    query = "select "+slt+", "+agg+" from "+frm+" group by "+grp
      
  cursor.execute(query)
  data_list = cursor.fetchall()
  
  colnames = [desc[0] for desc in cursor.description]
  return jsonify(result = "success", result2 = data_list, result3=colnames)

######
@app.route('/explanation',methods=['EXP'])
def explanation():
    # data = request.get_json()

    # tdArr = data["tdArr"]
    # colNum = data["colNum"]
    # colData = data["colData"]
    # rangelen = len(tdArr)

    # tmp1 = []
    # tmp2 = []
  
    # for i in range(0, rangelen):
    #   if i<colNum:
    #       tmp1.append(tdArr[i])
    #   else:
    #       tmp2.append(tdArr[i])

    #uQuery = "provenance of ("+query+");"
    #map_yes = ""
    #map_no = ""

    # run_experiment(conn=globals()['conn'],
    #                 result_schema='demotest',
    #                 user_query=(uQuery, 'test'),
    #                 user_questions = ["season_name='2015-16'","season_name='2012-13'"],
    #                 user_questions_map = {'yes':'2015-16', 'no':'2012-13'},
    #                 user_specified_attrs=[('team','team'),('season','season_name')])

    run_experiment(conn=globals()['conn'])

    globals()['conn'] = pg2.connect(database=db_name, 
            user=db_user, 
            password=db_pswd,
            port=db_port,
            host=db_host)
    globals()['conn'].autocommit = True
    globals()['cursor2'] = conn.cursor()

    query2 = "select p_desc from demotest.global_results"
    cursor2.execute(query2)
    exp_list = cursor2.fetchall()

    return jsonify(result = "success-explanation", result2 = exp_list)


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


    



if __name__ == '__main__':
    app.run(debug=True)
