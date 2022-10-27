import os

rate = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]
stop = [1, 3, 5, 7, 9]

# cmmd = 'python3 experiments.py -H 127.0.0.1 -P 5433 -U postgres -d nba_db -p 1234 -u True -R 0.8 -T 3 - D testingname'

cmmd = 'python3 experiments.py -H 127.0.0.1 -P 5433 -U postgres -d nba_db -p 1234 -u True -R '

for i in rate:
    for j in stop:
        result_schema_name = 'testing_'+str(i).replace('.','')+'_'+str(j)
        cmmd = cmmd + str(i) + ' -T ' + str(j) + ' -D ' + result_schema_name
        os.system(cmmd)





