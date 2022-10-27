import psycopg2
from matplotlib import pyplot as plt

conn = psycopg2.connect(database='nba_db', user='postgres', password='1234', port='5433', host='127.0.0.1')
cur = conn.cursor()

############################################
############################################
# ## Plot for CaJaDe_prev - show the testing results in a single plot
#prev_jg_x = []
#prev_jg_y = []
#prev_ptt_x = []
#prev_ptt_y = []
#prev_jg_total_time = 0

# # runtime in the Join Graph generation
#get_prev_jg_results = "SELECT jg_enumeration, jg_hashing, jg_validtaion FROM exp_2022_10_24_03_47_04.time_and_params;"
#cur.execute(get_prev_jg_results)
#prev_jg_results = cur.fetchall()

#for item in prev_jg_results[0]:
#  prev_jg_total_time+=float(item)
#prev_jg_total_time = round(prev_jg_total_time)
#print(prev_jg_total_time)
#for i in range(0,prev_jg_total_time+1):
#  prev_jg_x.append(i)
#  prev_jg_y.append(0)

#plt.plot(prev_jg_x, prev_jg_y, '--')

# # runtime in the Pattern generation
#get_prev_ptt_results = "SELECT * FROM exp_2022_10_24_03_47_04.cajade_prev_testing;"
#cur.execute(get_prev_ptt_results)
#prev_ptt_results = cur.fetchall()

#for item in prev_ptt_results:
#  prev_ptt_x.append(item[0]+prev_jg_total_time)
#  prev_ptt_y.append(item[1])
  
#plt.plot(prev_ptt_x, prev_ptt_y, '-')

#plt.xlabel('time(sec)')
#plt.ylabel('# explanation')
#plt.title('Runtime experiment of the CaJaDE system')
# #plt.show()
#plt.savefig('prev_result.png')

#conn.commit()

############################################
############################################
## Plot for CaJaDe_new - Each # rates has five different stops
rate_list = ['10', '09', '08', '07', '06', '05']
stop_list = [1, 3, 5, 7, 9]
marker_shape = ['o', 's', 'v', 'p', '*']
marker_color = ['b','g','r','c','m']

sub_plots = plt.subplots(2,3,figsize=(30,15))
fig = sub_plots[0]
graph = sub_plots[1]

# select * from testing_05_1.cajade_new_testing;
# select jg_e_cum, jg_h_cum, jg_v_cum, jg_s_cum, jg_utime_cum from testing_10_1.time_and_params;

for k in range(0,len(rate_list)): 
#k = rate_list[0]
#sub_plots = plt.subplots(2,3,figsize=(30,15))
#fig = sub_plots[0]
#graph = sub_plots[1]

  for i in range(0,len(stop_list)):
    new_jg_x = []
    new_jg_y = []
    new_ptt_x = []
    new_ptt_y = []
    new_jg_total_time = 0
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

    if k<3:
      graph[0][k].plot(new_jg_x, new_jg_y, marker_color[i]+'--')
    else:
      graph[1][k-3].plot(new_jg_x, new_jg_y, marker_color[i]+'--')

    # runtime in the Pattern generation
    get_new_ptt_results = "SELECT * FROM "+schema_name+".cajade_new_testing;"
    cur.execute(get_new_ptt_results)
    new_ptt_results = cur.fetchall()

    for item in new_ptt_results:
      new_ptt_x.append(item[0]+new_jg_total_time)
      new_ptt_y.append(item[1])

    if k<3:
      graph[0][k].plot(new_ptt_x, new_ptt_y, marker_color[i]+marker_shape[i]+'-',label='stop #'+str(stop_list[i]))
      graph[0][k].set_title('rate_'+str(rate_list[k])+'_result')
      graph[0][k].set_xlabel('time(sec)')
      graph[0][k].set_ylabel('# explanation')
    else:
      graph[1][k-3].plot(new_ptt_x, new_ptt_y, marker_color[i]+marker_shape[i]+'-',label='stop #'+str(stop_list[i]))
      graph[1][k-3].set_title('rate_'+str(rate_list[k])+' result')
      graph[1][k-3].set_xlabel('time(sec)')
      graph[1][k-3].set_ylabel('# explanation')
    
#plt.xlabel('time(sec)')
#plt.ylabel('# explanation')
#plt.title('Runtime experiment of the CaJaDE system')
fig.suptitle('Runtime experiment of the new approach')
fig.tight_layout(pad=5)
plt.savefig('new_result.png')
conn.commit()



############################################
############################################
## Plot for both







############################################
############################################
cur.close()
conn.close()
