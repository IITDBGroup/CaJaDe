import os

rate = [1.0, 0.95, 0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6, 0.55, 0.5] #11
stop = [1, 3, 5, 7, 9, 11, 13, 15, 17, 20] #10

# cmmd = 'python3 experiments.py -H 127.0.0.1 -P 5433 -U postgres -d nba_db -p 1234 -u True -R 0.8 -T 3 - D testingname'

cmmd = 'python3 experiments.py -H 127.0.0.1 -P 5433 -U postgres -d nba_db -p 1234 -u True -R '

for i in rate:
    for j in stop:
        result_schema_name = 'exp_'+str(i)+'_'+str(j)
        cmmd = cmmd + str(i) + ' -T ' + str(j) + ' -D ' + result_schema_name
        os.system(cmmd)





