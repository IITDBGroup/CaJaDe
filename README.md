# **CaJaDe**
The source codes are located  **src**/  folder. After importing databases to **postgres**, simply go to **src/** folder, 

It is recommended that you use **virtualenv** or similar virtual environment to deploy the code,

1. once you are in the virtual environment, run

`pip install requirements.txt`

2. **CaJaDe** uses ***GProM*** as the backend provenance generation system, please refer to [GProM](https://github.com/IITDBGroup/gprom) to install along with your **Postgresql** before going forward 
3. We use **NBA** dataset as the demo dataset. User can download different sizes of the dataset in [here](https://drive.google.com/drive/folders/10IkUk9n7vT-2OHQCZKi1apr8y_IrnbvW?usp=sharing). After extracting the `.sql` file, run `psql -h  [hostname] -p [port] -U [username] -d [your precreated db name] < [your .sql file]`to import database to your local **Postgresql**, e.g, `psql -h localhost -p 5432 -U postgres -d nba_db < nba.sql` 
4. After you've done first 3 steps, you are ready to go :)

run  

`python experiments.py -h`

user could select specify the following flags to run experiments

```
usage: experiments.py [-h] [-M] [-F] [-o] [-s] [-m] [-H] [-P] [-t] -U  -p  -d

Running experiments of CaJaDe

optional arguments:
  -h, --help            show this help message and exit
  -M , --maximum_edges 
                        Maximum number of edges allowed in a join graph
                        (default: 3)
  -F , --f1_sample_rate 
                        Sample rate of apt when calculating the f1 score
                        (default: 1.0)
  -o , --optimized      use opt or not (y: yes, n: no), (default: y)
  -s , --db_size        scale factor of database, (default: 1.0)
  -m , --min_recall_threshold 
                        recall threshold when calculating f1 score (default:
                        1.0)
  -H , --db_host        database host, (default: localhost)
  -P , --port           database port, (default: 5432)
  -t , --f1_calc_type   f1 score type (s sample, o original), (default: s)

required named arguments:
  -U , --user_name      owner of the database (required)
  -p , --password       password to the database (required)
  -d , --db_name        database name (required)
```

and follow the instructions popped out to run the experiment.

The result of the experiment will be saved under the schema name `exp_%Y_%m_%d_%H_%M_%S` inside the database user chooses to use.



