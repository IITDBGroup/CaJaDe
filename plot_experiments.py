import psycopg2
from matplotlib import pyplot as plt

conn = psycopg2.connect(database='nba_db', user='postgres', password='1234', port='5433', host='127.0.0.1')
cur = conn.cursor()

############################################
############################################
# ## Plot for CaJaDe_prev - show the testing results in a single plot
# prev_jg_x = []
# prev_jg_y = []
# prev_ptt_x = []
# prev_ptt_y = []
# prev_jg_total_time = 0

# # runtime in the Join Graph generation
# get_prev_jg_results = "SELECT jg_enumeration, jg_hashing, jg_validtaion FROM exp_2022_10_24_03_47_04.time_and_params;"
# cur.execute(get_prev_jg_results)
# prev_jg_results = cur.fetchall()

# for item in prev_jg_results[0]:
#   prev_jg_total_time+=float(item)
# prev_jg_total_time = round(prev_jg_total_time)

# for i in range(0,prev_jg_total_time+1):
#   prev_jg_x.append(i)
#   prev_jg_y.append(0)

# plt.plot(prev_jg_x, prev_jg_y, 'o--')

# # runtime in the Pattern generation
# get_prev_ptt_results = "SELECT * FROM exp_2022_10_24_03_47_04.cajade_prev_testing;"
# cur.execute(get_prev_ptt_results)
# prev_ptt_results = cur.fetchall()

# for item in prev_ptt_results:
#   prev_ptt_x.append(item[0]+prev_jg_total_time)
#   prev_ptt_y.append(item[1])
  
# plt.plot(prev_ptt_x, prev_ptt_y, 'o-')

# plt.xlabel('time(sec)')
# plt.ylabel('# explanation')
# plt.title('Runtime experiment of the CaJaDE system')
# #plt.show()
# plt.savefig('prev_result.png')

# conn.commit()

############################################
############################################
## Plot for CaJaDe_new - Each # rate has five different stop
rate_list = ['10', '09', '08', '07', '06', '05']
stop_list = [1, 3, 5, 7, 9]
plot_option = ['bo', 'gs', 'rv', 'cp', 'm*']

# select * from testing_05_1.cajade_new_testing;
# select jg_e_cum, jg_h_cum, jg_v_cum, jg_s_cum, jg_utime_cum from testing_10_1.time_and_params;

for k in range(0,len(rate_list)):
  plt.subplot(2,3,k+1)
  for i in range(0,len(stop_list)):
    new_jg_x = []
    new_jg_y = []
    new_ptt_x = []
    new_ptt_y = []
    new_jg_total_time = 0
    schema_name = "testing_"+k+"_"+stop_list[i]

    # runtime in the Join Graph generation
    get_new_jg_results = "SELECT jg_e_cum, jg_h_cum, jg_v_cum, jg_s_cum, jg_utime_cum FROM "+schema_name+".time_and_params;"
    cur.execute(get_new_jg_results)
    new_jg_results = cur.fetchall()

    for item in new_jg_results[0]:
      new_jg_total_time+=float(item)
      new_jg_total_time = round(new_jg_total_time)

    for i in range(0,new_jg_total_time+1):
      new_jg_x.append(i)
      new_jg_y.append(0)

    plt.plot(new_jg_x, new_jg_y, plot_option[i]+'--')

    # runtime in the Pattern generation
    get_new_ptt_results = "SELECT * FROM "+schema_name+".cajade_new_testing;"
    cur.execute(get_new_ptt_results)
    new_ptt_results = cur.fetchall()

    for item in new_ptt_results:
      new_ptt_x.append(item[0]+new_jg_total_time)
      new_ptt_y.append(item[1])

    plt.plot(new_ptt_x, new_ptt_y, plot_option[i]+'-')
    
plt.xlabel('time(sec)')
plt.ylabel('# explanation')
plt.title('Runtime experiment of the CaJaDE system')
plt.savefig('new_result.png')
conn.commit()



############################################
############################################
## Plot for both







############################################
############################################
cur.close()
conn.close()
