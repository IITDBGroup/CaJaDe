from flask import Flask
from flask import render_template, request, jsonify, redirect, url_for, flash
import psycopg2 as pg2
from networkx import MultiGraph
from src.sg_generator import Schema_Graph_Generator
from src.experiments import run_experiment
from flask.logging import default_handler
import logging

logger = logging.getLogger()
logger.addHandler(default_handler)

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
  where = data["where"]
  grp = data["grp"]

  global query


  query = f"""
  SELECT {slt} \
  {", {}".format(agg) if agg!="" else ""} FROM {frm} \
  {"WHERE {}".format(where) if where !="" else ""} \
  {"GROUP BY {}".format(grp) if grp !="" else ""} \
  """
  logger.debug(query)
  cursor.execute(query)
  data_list = cursor.fetchall()
  
  colnames = [desc[0] for desc in cursor.description]
  return jsonify(result = "success", result2 = data_list, result3=colnames)

######
@app.route('/explanation',methods=['EXP'])
def explanation():
    data = request.get_json()

    # tdArr = data["tdArr"]
    # colNum = data["colNum"]
    # colData = data["colData"]
    # rangelen = len(tdArr)

    # tmp1 = []
    # tmp2 = []
    
    # logger.debug(query)


    # for i in range(0, rangelen):
    #   if i<colNum:
    #       tmp1.append(tdArr[i])
    #   else:
    #       tmp2.append(tdArr[i])

    uQuery = "provenance of ("+query+");"
    #map_yes = ""
    #map_no = ""

    # run_experiment(conn=globals()['conn'],
    #                 result_schema='demotest',
    #                 user_query=(uQuery, 'test'),
    #                 user_questions = ["season_name='2015-16'","season_name='2012-13'"],
    #                 user_questions_map = {'yes':'2015-16', 'no':'2012-13'},
    #                 user_specified_attrs=[('team','team'),('season','season_name')])
    # print(uQuery)
    # run_experiment(conn=globals()['conn'],
    #             result_schema='demotest',
    #             user_query=(uQuery, 'test'),
    #             user_questions = ["team='BOS'","team='DET'"],
    #             user_questions_map = {'yes':'BOS', 'no':'DET'},
    #             user_specified_attrs=[('team','team')]
    #             )
    
    # def run_experiment(conn=None,
    #                result_schema='demotest',
    #                user_query = ("provenance of (select count(*) as win, s.season_name from team t, game g, season s where t.team_id = g.winner_id and g.season_id = s.season_id and t.team= 'GSW' group by s.season_name);",'test'),
    #                user_questions = ["season_name='2015-16'","season_name='2012-13'"],
    #                user_questions_map = {'yes':'2015-16', 'no':'2012-13'},
    #                user_specified_attrs=[('team','team'),('season','season_name')],
    #                user_name='juseung',
    #                password='1234',
    #                host='localhost',
    #                port='5432',
    #                dbname='nba', 
    #                sample_rate_for_s=0.1,
    #                lca_s_max_size=100,
    #                lca_s_min_size=100,
    #                maximum_edges=1,
    #                min_recall_threshold=0.2,
    #                numercial_attr_filter_method='y',
    #                f1_sample_rate=0.3,
    #                f1_sample_type='s.0',
    #                exclude_high_cost_jg = (False, 'f'),
    #                f1_calculation_type = 'o',
    #                user_assigned_max_num_pred = 3,
    #                f1_min_sample_size_threshold=100,
    #                lca_eval_mode=False,
    #                statstracker=ExperimentParams()):


    run_experiment(conn=globals()['conn'])

    # globals()['conn'] = pg2.connect(database=db_name, 
    #         user=db_user, 
    #         password=db_pswd,
    #         port=db_port,
    #         host=db_host)
    # globals()['conn'].autocommit = True
    # globals()['cursor2'] = conn.cursor()

    query2 = "select p_desc from demotest.global_results"
    globals()['cursor'].execute(query2)
    # cursor2.execute(query2)
    exp_list = globals()['cursor'].fetchall()

    # exp_list = cursor2.fetchall()

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
