# SIGMOD Reproducibility for Paper "Putting Things into Context: Rich Explanations for Query Answers using Join Graphs"

[https://github.com/IITDBGroup/CaJaDe/tree/sigmod_reproducibility](https://github.com/IITDBGroup/CaJaDe/tree/sigmod_reproducibility)


# A) Source Code Info

The **CaJaDE** system is written in `Python` and uses [**PostgreSQL**](https://www.postgresql.org/) as a backend for storage.  We use [**GProM**](https://github.com/IITDBGroup/gprom/tree/gprom_in_cajade) as the tool to generate the provenance of the query. 

As we have some comparison experiments with 2 previous work. we provide the access to those work as well: 

- For **CAPE** ([paper link](https://dl.acm.org/doi/10.1145/3299869.3300066])), you could access cape repository from [here][https://github.com/IITDBGroup/cape] `master` branch
- For **Explanation Table** ([paper link][https://dl.acm.org/doi/10.14778/2735461.2735467]) has no public repo, but we contacted the authors and got the source code. we include the code in a docker image which we discuss in detail in **Section D)**

The **CaJaDE** package installs a library as well as a commandline tool `cajadexplain`. This tool provides the arguments needed to run a query in a given database, with provided "2 point" user question, and some parameters to produce the explanations and save the explanations in the given database schema

- Repository: https://github.com/IITDBGroup/cape (branch `SIGMOD-reproducibility`)
- Programming Language: Python
- Additional Programming Language info: we are requiring Python3. Specifically, we are using Python 3.8.10 in this reproducibility submission.
- OS for Experiments: Linux (required for docker)
- To summarize, in order to reproduce all the experiment results the following source codes are required :
    - [**GProM**](https://github.com/IITDBGroup/gprom/tree/gprom_in_cajade) 
    - [**CAPE**](https://github.com/IITDBGroup/cape)
    - [**CaJaDE**](https://github.com/IITDBGroup/CaJaDe/tree/sigmod_reproducibility) (this repository will contain Explanation Table Source code)
    -  [**PostgreSQL**](https://www.postgresql.org/) 

# B) Datasets

- NBA dataset: NBA (National Basketball Association) dataset were extracted from [this cite](http://www.pbpstats.com/). This dataset will be available in the reproducibility repo
- MIMIC dataset:  in order to access MIMIC (Medical Information Mart for Intensive Care) dataset, it requires user to finish the steps listed [here](https://mimic.mit.edu/docs/gettingstarted/) before starting working with the data. We do not provide the dataset in this reproducibility repository due to the policy. However, we will provide the scripts needed to prepare the processed MIMIC dataset used in the experiments. 

# C) Hardware Info

All runtime experiments were executed on a server with the following specs:

| Element          | Description                                                                   |
|------------------|-------------------------------------------------------------------------------|
| CPU              | 2 x AMD Opteron(tm) Processor 4238, 3.3Ghz                                    |
| Caches (per CPU) | L1 (288KiB), L2 (6 MiB), L3 (6MiB)                                            |
| Memory           | 128GB (DDR3 1333MHz)                                                          |
| RAID Controller  | LSI Logic / Symbios Logic MegaRAID SAS 2108 [Liberator] (rev 05), 512MB cache |
| RAID Config      | 4 x 1TB, configured as RAID 5                                                 |
| Disks            | 4 x 1TB 7.2K RPM Near-Line SAS 6Gbps (DELL CONSTELLATION ES.3)                |


# D) Installation, Setup, and Running Experiments

Please follow these instructions to install the system and datasets for reproducibility. We use 4 docker images for  reproducibility: 

1. a container running CaJade, GProm
2. a container running postgres (including the datasets for the experiments).
3. a container running CAPE
4. a container running Explanation Table

## Prerequisites ##

- You need to have [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/install/) installed on your system (please use linux so docker can use sufficient memory resources  and does not have to run in a VM).

## Clone git Repository

Please clone the Cape git repository and check out the `sigmod-reproducibility` branch. This branch contains Cape as well as scripts for running experiments and plotting results.

~~~shell
git clone --single-branch --branch sigmod_reproducibility https://github.com/IITDBGroup/CaJaDe.git
~~~

To check whether it was cloned correctly run:

~~~shell
cd CaJaDe
tree -d
~~~

this should produce an output like this: (**TODO**: CLEAN UP!)

~~~shell
.
├── build
│   ├── bdist.linux-x86_64
│   └── lib
│       ├── demo
│       └── src
├── CaJade.egg-info
├── cape_compare
├── demo
│   └── __pycache__
├── dist
├── docker
│   ├── cajade
│   ├── cape
│   └── postgres
├── exp_graph
│   ├── cape_compare
│   └── revision
├── Mimic_PlayGround
├── __pycache__
├── reproduce
├── revision_materials
├── src
│   └── __pycache__
├── Top20s
└── user_study
    ├── new_tables_nba
    └── pics


~~~

## Start-up Docker Cluster

We are using `docker-compose` for this. Switch to the `docker` directory inside the git repository you have cloned.

~~~shell
cd docker
~~~

Now run docker-compose which will create a `cape-system` and a `postgres-cape` container:

~~~shell
docker-compose up -d
~~~

To test whether the containers are running do:

~~~shell
docker ps
~~~

You should see these two containers (the IDs would be different of course):

~~~shell
CONTAINER ID   IMAGE                                                          COMMAND                  CREATED         STATUS         PORTS                                       NAMES
85b6a9113863   jayli2018/2021-sigmod-reproducibility-cajade_cape:latest       "python3"                2 minutes ago   Up 2 minutes                                               cajade-cape
91a554935b55   jayli2018/2021-sigmod-reproducibility-cajade_systems:latest    "bash"                   2 minutes ago   Up 2 minutes                                               cajade-main
8cc791acff83   jayli2018/2021-sigmod-reproducibility-cajade_et:latest         "python2"                2 minutes ago   Up 2 minutes                                               cajade-et
bd3cc05dc408   jayli2018/2021-sigmod-reproducibility-cajade_postgres:latest   "docker-entrypoint.s…"   2 minutes ago   Up 2 minutes   0.0.0.0:5432->5432/tcp, :::5432->5432/tcp   cajade-psql

~~~

## Run Experiments

There is a main script for driving the experiments that is run from within the `cape-system` container. This script copies results to the directory from which you ran `docker-compose`. Partial experiments can be restarted and existing results (CSV and PDF files) will not be overwritten. This is because the whole experiments will run for a few days. Note that the script only runs the each experiment once (instead of 3 times as we did for the paper) to keep the runtime feasible. You can overwrite this behaviour by setting an environment variable when running the script.

To start the experiments:

~~~shell
docker exec -ti -w /usr/local/cape/reproduce/ cape-system /usr/local/cape/reproduce/script.sh
~~~

Over time you should see CSV and PDF files pop up in the `docker` folder in the cloned git repository.
If you want to run additional repetitions for the long-running mining experiments, then you can set the number of repetitions like this, e.g., setting it to 3 repetitions:

~~~shell
docker exec -ti -w /usr/local/cape/reproduce/ -e rep=3 cape-system /usr/local/cape/reproduce/script.sh
~~~

In our experiments we evaluated three things:

- performance of the offline pattern mining algorithm
- performance of the online explanation generation algorithm
- quality of the generated explanations wrt. to a known ground truth

All generated result files will be in `docker` folder within the cape repository on your local machine. See Section F for detail.

# E) Suggestions and Instructions for Alternative Experiments

For convenience, we provide the single script that runs all experiments. Creating explanations for outliers in cape consists of two steps. There is an offline mining phase that detects patterns in a dataset and an online explanation generation phase that uses the patterns to create an explanation for a user questions. To run different parameter settings, you can use the commandline client to run these phases (`capexplain COMMAND -help` lists all options that are available for a particular command, e.g., `mine`). Furthermore, we provide a GUI for exploring explanations. Feel free to use it for generating explanations for additional queries / user questions not covered in the experiments.


## Pattern Mining

The pattern mining algorithm takes multiple configuration parameters. You can run the algorithm with different parameter settings. Here are a few suggestions. We describe the parameters in more details below.

- this command repeats a short experiment over the publication table, but output details of what the miner is doing (using `-l DEBUG` parameter that show a lot of debug outputs. At the end cape will show statistics about runtime and the detected patterns.

~~~shell
docker exec -ti cape-system /usr/local/bin/capexplain mine -h postgres-cape -u antiprov -p antiprov -d antiprov -P 5436 -t pub_10000 --algorithm cube --local-support 15 --global-support 15 --show-progress False -l DEBUG
~~~

- run the algorithm for one subset of the crime table. Run the command multiple times changing the *local support threshold* to 5, 10, and 15 and observe how the number of detected local and global patterns change. See our paper to understand the meaning of the local support threshold.

~~~shell
docker exec -ti cape-system /usr/local/bin/capexplain mine -h postgres-cape -u antiprov -p antiprov -d antiprov -P 5436 -t crime_exp_5 --algorithm optimized --local-support 55 --global-support 10 --show-progress True
~~~

- we included additional dataset in the postgres container that was not used in the experiments. The `potholes` dataset records when potholes were at what location in Chicago. The `cta` dataset records traffic at Chicago public transportation stations. For instance, you could try:

~~~shell
docker exec -ti cape-system /usr/local/bin/capexplain mine -h postgres-cape -u antiprov -p antiprov -d antiprov -P 5436 -t pot_10000 --algorithm optimized --local-support 15 --global-support 20 --show-progress True
~~~

and

~~~shell
docker exec -ti cape-system /usr/local/bin/capexplain mine -h postgres-cape -u antiprov -p antiprov -d antiprov -P 5436 -t cta_100000 --algorithm optimized --local-support 15 --global-support 20 --show-progress True
~~~

### Mining Pattern Command

Use `capexplain mine [OPTIONS]` to mine patterns. Cape will store the discovered patterns in the database. The "mined" patterns will be stored in a created schema called `pattern`, and the pattern tables generated after running `mine` command are `pattern.{target_table}_global` and `pattern.{target_table}_local`. At the minimum you have to tell Cape how to connect to the database you want to use and which table it should generate patterns for. Run `capexplain help mine` to get a list of all supported options for the mine command. The options needed to specify the target table and database connection are:

~~~shell
-h ,--host <arg>               - database connection host IP address (DEFAULT: 127.0.0.1)
-u ,--user <arg>               - database connection user (DEFAULT: postgres)
-p ,--password <arg>           - database connection password
-d ,--db <arg>                 - database name (DEFAULT: postgres)
-P ,--port <arg>               - database connection port (DEFAULT: 5432)
-t ,--target-table <arg>       - mine patterns for this table
~~~

For instance, if you run a postgres server locally (default) with user `postgres` (default), password `test`, and want to mine patterns for a table `employees` in database `mydb`, then run:

~~~shell
capexplain mine -p test -d mydb -t employees
~~~

### Mining Algorithm Parameters ###

Cape's mining algorithm takes the following arguments:

~~~shell
--gof-const <arg>              - goodness-of-fit threshold for constant regression (DEFAULT: 0.1)
--gof-linear <arg>             - goodness-of-fit threshold for linear regression (DEFAULT: 0.1)
--confidence <arg>             - global confidence threshold
-r ,--regpackage <arg>         - regression analysis package to use {'statsmodels', 'sklearn'} (DEFAULT: statsmodels)
--local-support <arg>          - local support threshold (DEFAULT: 10)
--global-support <arg>         - global support thresh (DEFAULT: 100)
-f ,--fd-optimizations <arg>   - activate functional dependency detection and optimizations (DEFAULT: False)
-a ,--algorithm <arg>          - algorithm to use for pattern mining {'naive', 'cube', 'share_grp', 'optimized'} (DEFAULT: optimized)
--show-progress <arg>          - show progress meters (DEFAULT: True)
--manual-config                - manually configure numeric-like string fields (treat fields as string or numeric?) (DEFAULT: False)
~~~



# G) Links and Contact Information #

Cape is developed by researchers at Illinois Institute of Technology and Duke University. For more information and publications see the Cape project page [http://www.cs.iit.edu/~dbgroup/projects/cape.html](http://www.cs.iit.edu/~dbgroup/projects/cape.html).

For questions about the experiments or `Cape` feel free to contact us:

- **Qitian Zeng** [qzeng3@hawk.iit.edu](qzeng3@hawk.iit.edu)
- **Zhengjie Miao** [zhengjie.miao@duke.edu](zhengjie.miao@duke.edu)


# H) Appendix - Cape Usage

Cape provides a single binary `capexplain` that support multiple subcommands. The general form is:

~~~shell
capexplain COMMAND [OPTIONS]
~~~

Options are specific to each subcommand. Use `capexplain help` to see a list of supported commands and `capexplain help COMMAND` get more detailed help for a subcommand.

## Overview ##

Cape currently only supports PostgreSQL as a backend database (version 9 or higher). To use Cape to explain an aggregation outlier, you first have to let cape find patterns for the table over which you are aggregating. This an offline step that only has to be executed only once for each table (unless you want to re-run pattern mining with different parameter settings). Afterwards, you can either use the commandline or Cape's UI to request explanations for an outlier in an aggregation query result.

## Mining Patterns ##

Use `capexplain mine [OPTIONS]` to mine patterns. Cape will store the discovered patterns in the database. The "mined" patterns will be stored in a created schema called `pattern`, and the pattern tables generated after running `mine` command are `pattern.{target_table}_global` and `pattern.{target_table}_local`. At the minimum you have to tell Cape how to connect to the database you want to use and which table it should generate patterns for. Run `capexplain help mine` to get a list of all supported options for the mine command. The options needed to specify the target table and database connection are:

~~~shell
-h ,--host <arg>               - database connection host IP address (DEFAULT: 127.0.0.1)
-u ,--user <arg>               - database connection user (DEFAULT: postgres)
-p ,--password <arg>           - database connection password
-d ,--db <arg>                 - database name (DEFAULT: postgres)
-P ,--port <arg>               - database connection port (DEFAULT: 5432)
-t ,--target-table <arg>       - mine patterns for this table
~~~

For instance, if you run a postgres server locally (default) with user `postgres` (default), password `test`, and want to mine patterns for a table `employees` in database `mydb`, then run:

~~~shell
capexplain mine -p test -d mydb -t employees
~~~

### Mining Algorithm Parameters ###

Cape's mining algorithm takes the following arguments:

~~~shell
--gof-const <arg>              - goodness-of-fit threshold for constant regression (DEFAULT: 0.1)
--gof-linear <arg>             - goodness-of-fit threshold for linear regression (DEFAULT: 0.1)
--confidence <arg>             - global confidence threshold
-r ,--regpackage <arg>         - regression analysis package to use {'statsmodels', 'sklearn'} (DEFAULT: statsmodels)
--local-support <arg>          - local support threshold (DEFAULT: 10)
--global-support <arg>         - global support thresh (DEFAULT: 100)
-f ,--fd-optimizations <arg>   - activate functional dependency detection and optimizations (DEFAULT: False)
-a ,--algorithm <arg>          - algorithm to use for pattern mining {'naive', 'cube', 'share_grp', 'optimized'} (DEFAULT: optimized)
--show-progress <arg>          - show progress meters (DEFAULT: True)
--manual-config                - manually configure numeric-like string fields (treat fields as string or numeric?) (DEFAULT: False)
~~~

### Running Our "crime" Data Example ###

We included a subset of the "Chicago Crime" dataset (https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/)
in our repository for user to play with. To import this dataset in your postgres databse, under `/testdb` directory, run the following command template:

~~~shell
psql -h <host> -U <user name> -d <local database name where you want to store our example table> < ~/cape/testdb/crime_demonstration.sql
~~~

then run the `capexplain` commands accordingly to explore this example.

## Explaining Outliers ###

To explain an aggregation outlier use `capexplain explain [OPTIONS]`.

~~~shell
-l ,--log <arg>                - select log level {DEBUG,INFO,WARNING,ERROR} (DEFAULT: ERROR)
--help                         - show this help message
-h ,--host <arg>               - database connection host IP address (DEFAULT: 127.0.0.1)
-u ,--user <arg>               - database connection user (DEFAULT: postgres)
-p ,--password <arg>           - database connection password
-d ,--db <arg>                 - database name (DEFAULT: postgres)
-P ,--port <arg>               - database connection port (DEFAULT: 5432)
--ptable <arg>                 - table storing aggregate regression patterns
--qtable <arg>                 - table storing aggregation query result
--ufile <arg>                  - file storing user question
-o ,--ofile <arg>              - file to write output to
-a ,--aggcolumn <arg>          - column that was input to the aggregation function
~~~

for `explain` option, besides the common options, user should give `--ptable`,the `pattern.{target_table}` and `--qtable`, the `target_table`. Also, we currently only allow user pass question through a `.txt` file, user need to put the question in the following format:

~~~shell
attribute1, attribute 2, attribute3...., direction
value1,value2,value3...., high/low
~~~
please refer to `input.txt` to look at an example.


## Starting the Explanation Explorer GUI ###

Cape comes with a graphical UI for running queries, selecting outliers of interest, and exploring patterns that are relevant for an outlier and browsing explanations generated by the system. You need to specify the Postgres server to connect to. The explorer can only generate explanations for queries over tables for which patterns have mined beforehand using `capexplain mine`.
Here is our demo video : (https://www.youtube.com/watch?v=gWqhIUrcwz8)

~~~shell
$ capexplain help gui
capexplain gui [OPTIONS]:
	Open the Cape graphical explanation explorer.

SUPPORTED OPTIONS:
-l ,--log <arg>                - select log level {DEBUG,INFO,WARNING,ERROR} (DEFAULT: ERROR)
--help                         - show this help message
-h ,--host <arg>               - database connection host IP address (DEFAULT: 127.0.0.1)
-u ,--user <arg>               - database connection user (DEFAULT: postgres)
-p ,--password <arg>           - database connection password
-d ,--db <arg>                 - database name (DEFAULT: postgres)
-P ,--port <arg>               - database connection port (DEFAULT: 5432)
~~~

For instance, if you run a postgres server locally (default) with user `postgres` (default), password `test`, and database `mydb`, then run:

~~~shell
capexplain gui -p test -d mydb
~~~
