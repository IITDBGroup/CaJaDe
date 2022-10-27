import psycopg2
from matplotlib import pyplot as plt

conn = psycopg2.connect(databse='nba_db', user='postgres', password='1234', port='5433', host='127.0.0.1')
cur = conn.cursor()

############################################
############################################
## Plot for CaJaDe_prev - show the testing results in a single plot
prev_x = []
prev_y = []
jg_total_time = 0

# runtime in the Join Graph generation
get_prev_jg_results = "SELECT jg_enumeration, jg_hashing, jg_validtaion FROM exp_2022_10_24_03_47_04.time_and_params;"
cur.execute(get_prev_jg_results)
prev_jg_results = cur.fetchall()

for item in prev_jg_results:
  jg_total_time+=item
jg_total_time = round(jg_total_time)

for i in range(0,jg_total_time+1):
  prev_x.append(i)
  prev_y.append(0)

# runtime in the Pattern generation
get_prev_ptt_results = "SELECT * FROM exp_2022_10_24_03_47_04.cajade_prev_testing;"
cur.execute(get_prev_ptt_results)
prev_ptt_results = cur.fetchall()

for item in prev_ptt_results:
  prev_x.append(item[0]+jg_total_time)
  prev_y.append(item[1])
  
plt.plot(prev_x, prev_y)

plt.xlabel('time(sec)')
plt.ylabel('# explanation')
plt.title('Runtime experiment of the CaJaDE system')
#plt.show()
plt.savefig('prev_result.png')

conn.commit()

## Plot 2) 


############################################
############################################
## Plot for CaJaDe_new
## rate = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]
## stop = [1, 3, 5, 7, 9]

## Plot 1) Each # stop has six different rate(from 1.0 to 0.5)


############################################
############################################
## Plot for both







############################################
############################################
cur.close()
conn.close()
