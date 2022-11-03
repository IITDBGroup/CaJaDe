import psycopg2
from matplotlib import pyplot as plt


############################################
############################################
# ## Plot for CaJaDe_prev - show the testing results in a single plot
def plot_cajade_orig(conn, cur):
  prev_jg_x = []
  prev_jg_y = []
  prev_ptt_x = []
  prev_ptt_y = []
  prev_jg_total_time = 0

  # runtime in the Join Graph generation
  get_prev_jg_results = "SELECT jg_enumeration, jg_hashing, jg_validtaion FROM exp_2022_10_24_03_47_04.time_and_params;"
  cur.execute(get_prev_jg_results)
  prev_jg_results = cur.fetchall()

  for item in prev_jg_results[0]:
    prev_jg_total_time+=float(item)
  prev_jg_total_time = round(prev_jg_total_time)
  # print(prev_jg_total_time)
  for i in range(0,prev_jg_total_time+1):
    prev_jg_x.append(i)
    prev_jg_y.append(0)

  plt.plot(prev_jg_x, prev_jg_y, 'b--', label='Join Graph Generation Time')

  # runtime in the Pattern generation
  get_prev_ptt_results = "SELECT * FROM exp_2022_10_24_03_47_04.cajade_prev_testing;"
  cur.execute(get_prev_ptt_results)
  prev_ptt_results = cur.fetchall()

  for item in prev_ptt_results:
    prev_ptt_x.append(item[0]+prev_jg_total_time)
    prev_ptt_y.append(item[1])
    
  plt.plot(prev_ptt_x, prev_ptt_y, 'b-',label='Pattern Generation Time')

  plt.xlabel('time(sec)')
  plt.ylabel('# explanation')
  # plt.title('Runtime experiment of the CaJaDE system')
  plt.legend()
  plt.savefig('original_cajade_result.png')

  conn.commit()

############################################
############################################
## Plot for CaJaDe_new - Each # rates has five different stops
def plot_new_app(conn, cur):
  rate_list = ['10','09','08','07','06','05']
  stop_list = [1, 3, 5, 7, 9]
  marker_shape = ['o', 's', 'v', 'p', '*']
  marker_color = ['k','g','r','c','m']

  sub_plots = plt.subplots(3,2,figsize=(15,10))
  fig = sub_plots[0]
  graph = sub_plots[1]
  #plt.figure(figsize=(15,10))

  for k in range(0,len(rate_list)): 
    for i in range(0,len(stop_list)):
      new_jg_x = []
      new_jg_y = []
      new_ptt_x = []
      new_ptt_y = []
      new_jg_total_time = 0
      idx = 0
      schema_name = "testing_"+str(rate_list[k])+"_"+str(stop_list[i])

      # runtime in the Join Graph generation
      get_new_jg_results = "SELECT jg_e_cum, jg_h_cum, jg_v_cum, jg_s_cum, jg_utime_cum FROM "+schema_name+".time_and_params;"
      cur.execute(get_new_jg_results)
      new_jg_results = cur.fetchall()

      for item in new_jg_results[0]:
        new_jg_total_time+=float(item)
        new_jg_total_time = round(new_jg_total_time)

      for j in range(0,new_jg_total_time+1):
        new_jg_x.append(j)
        new_jg_y.append(0)

      graph[k//2][k%2].plot(new_jg_x,new_jg_y, marker_color[i]+'--')
      #plt.subplot(3,2,k+1)
      #plt.plot(new_jg_x, new_jg_y, marker_color[i]+'--')

      # runtime in the Pattern generation
      get_new_ptt_results = "SELECT * FROM "+schema_name+".cajade_new_testing;"
      cur.execute(get_new_ptt_results)
      new_ptt_results = cur.fetchall()

      for item in new_ptt_results:
        new_ptt_x.append(item[0]+new_jg_total_time)
        new_ptt_y.append(item[1])

      graph[k//2][k%2].plot(new_ptt_x, new_ptt_y, marker_color[i]+marker_shape[i]+'-',label='stop #'+str(stop_list[i]))
      #plt.subplot(3,2,k+1)
      #plt.plot(new_ptt_x, new_ptt_y, marker_color[i]+marker_shape[i]+'-',label='stop #'+str(stop_list[i]))
      graph[k//2][k%2].set_title('rate_'+str(rate_list[k])+'_result')
      graph[k//2][k%2].set_xlabel('time(sec)')
      graph[k//2][k%2].set_ylabel('# explanation')
      #graph[k//2][k%2]
      #plt.legend()

  # fig.suptitle('Runtime experiment of the new approach')
  fig.tight_layout(pad=2)
  #plt.savefig('new_approach_'+str(rate_list[k])+'_result_mod.png')
  #plt.subplot_tool()
  #plt.subplots_adjust(left=0.1, bottom=2.1, right=0.9, top=2.9, wspace=0.4, hspace=0.4)
  plt.savefig('new_approach_plots_new.png')
  conn.commit()

############################################
############################################
## Plot for both
## pick one rate(1.0) and integrate it into the plot with original CaJaDE
def plot_both(conn, cur):
  prev_jg_x = []
  prev_jg_y = []
  prev_ptt_x = []
  prev_ptt_y = []
  prev_jg_total_time = 0

  rate = '10'
  stop_list = [1, 3, 5, 7, 9]
  marker_shape = ['o', 's', 'v', 'p', '*']
  marker_color = ['k','g','r','c','m']
  plt.figure(figsize=(10,15))
  plt.rc('font', size=20)
  # runtime in the Join Graph generation
  get_prev_jg_results = "SELECT jg_enumeration, jg_hashing, jg_validtaion FROM exp_2022_10_24_03_47_04.time_and_params;"
  cur.execute(get_prev_jg_results)
  prev_jg_results = cur.fetchall()

  for item in prev_jg_results[0]:
    prev_jg_total_time+=float(item)
  prev_jg_total_time = round(prev_jg_total_time)
  # print(prev_jg_total_time)
  for i in range(0,prev_jg_total_time+1):
    prev_jg_x.append(i)
    prev_jg_y.append(0)

  plt.plot(prev_jg_x, prev_jg_y, 'b--', label='Join Graph Generation Time')

  # runtime in the Pattern generation
  get_prev_ptt_results = "SELECT * FROM exp_2022_10_24_03_47_04.cajade_prev_testing;"
  cur.execute(get_prev_ptt_results)
  prev_ptt_results = cur.fetchall()

  for item in prev_ptt_results:
    #if (item[0]+prev_jg_total_time)>240:
      #break
    prev_ptt_x.append(item[0]+prev_jg_total_time)
    prev_ptt_y.append(item[1])
    
  plt.plot(prev_ptt_x, prev_ptt_y, 'b-',label='Pattern Generation Time')

  for i in range(0,len(stop_list)):
    new_jg_x = []
    new_jg_y = []
    new_ptt_x = []
    new_ptt_y = []
    new_jg_total_time = 0
    idx = 0
    schema_name = "testing_10_"+str(stop_list[i])

    # runtime in the Join Graph generation
    get_new_jg_results = "SELECT jg_e_cum, jg_h_cum, jg_v_cum, jg_s_cum, jg_utime_cum FROM "+schema_name+".time_and_params;"
    cur.execute(get_new_jg_results)
    new_jg_results = cur.fetchall()

    for item in new_jg_results[0]:
      new_jg_total_time+=float(item)
      new_jg_total_time = round(new_jg_total_time)

    for j in range(0,new_jg_total_time+1):
      new_jg_x.append(j)
      new_jg_y.append(0)

    plt.plot(new_jg_x, new_jg_y, marker_color[i]+'--')

    # runtime in the Pattern generation
    get_new_ptt_results = "SELECT * FROM "+schema_name+".cajade_new_testing;"
    cur.execute(get_new_ptt_results)
    new_ptt_results = cur.fetchall()

    for item in new_ptt_results:
      #if (item[0]+new_jg_total_time)>240:
        #break
      new_ptt_x.append(item[0]+new_jg_total_time)
      new_ptt_y.append(item[1])

    plt.plot(new_ptt_x, new_ptt_y, marker_color[i]+marker_shape[i]+'-',label='stop #'+str(stop_list[i]))
    plt.legend()

  plt.xlabel('time(sec)')
  plt.ylabel('# explanation')
  # plt.title('Runtime experiment of the CaJaDE system')
  plt.legend()
  plt.savefig('integ_cajade_result_bigsize2.png')

  conn.commit()

############################################
############################################
conn = psycopg2.connect(database='nba_db', user='postgres', password='1234', port='5433', host='127.0.0.1')
cur = conn.cursor()
#plot_cajade_orig(conn, cur)
#plot_new_app(conn, cur)
plot_both(conn, cur)
cur.close()
conn.close()
