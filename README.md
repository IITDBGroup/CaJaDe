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
- Additional Programming Language info: we are requiring Python3. Tested versions are Python 3.6 and Python 3.8.
- OS for Experiments: Linux (required for docker)
- Required libraries/packages:
    - `tkinter` which requires a system package to be installed (see below)
    - `postgresql` as a database backend

# B) Datasets

- NBA dataset: NBA (National Basketball Association) dataset were extracted from [this cite](http://www.pbpstats.com/). This dataset will be available in the reproducibility repo
- MIMIC dataset:  in order to access MIMIC (Medical Information Mart for Intensive Care) dataset, it requires user to finish the steps listed [here](https://mimic.mit.edu/docs/gettingstarted/) before starting working with the data. We do not provide the dataset in this reproducibility repository due to the policy. However, we provide the scripts needed to prepare the processed MIMIC dataset used in the experiments.

https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2)

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

Please follow these instructions to install the system and datasets for reproducibility. Please see below for an standard installation with pip. We use two docker images for  reproducibility: 1) a container running cape and 2) a container running postgres (including the datasets for the experiments).

## Prerequisites ##

- You need to have [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/install/) installed on your system (please use linux so docker can use sufficient memory resources  and does not have to run in a VM).

## Clone git Repository

Please clone the Cape git repository and check out the `sigmod-reproducibility` branch. This branch contains Cape as well as scripts for running experiments and plotting results.

~~~shell
git clone --single-branch --branch sigmod-reproducibility https://github.com/IITDBGroup/cape.git
~~~

To check whether it was cloned correctly run:

~~~shell
cd cape
tree -d
~~~

this should produce an output like this:

~~~shell
.
├── capexplain
│   ├── cl
│   ├── database
│   ├── dev
│   ├── explain
│   ├── explanation_model
│   ├── fd
│   ├── gui
│   ├── pattern_miner
│   ├── pattern_model
│   └── similarity
├── docker
│   ├── cape
│   └── postgres
├── images
├── reproduce
│   ├── experiments
│   ├── expl_param_exp
│   │   ├── explanation_model
│   │   ├── input
│   │   └── similarity
│   ├── expl_perf_exp
│   │   └── input
│   └── expl_qual_exp
│       └── input
└── testdb
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
CONTAINER ID        IMAGE                                                         COMMAND                  CREATED             STATUS              PORTS                              NAMES
14cd28b898c0        289a14cb79dc                                                  "/mesleeps.sh"           About an hour ago   Up About an hour                                       cape-system
d603c8e78410        iitdbgroup/2019-sigmod-reproducibility-cape-postgres:latest   "docker-entrypoint.s…"   About an hour ago   Up About an hour    5432/tcp, 0.0.0.0:5436->5436/tcp   postgres-cape
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

## Explanation Generation

- TODO how to subsample the tables (create script), how to generate explanations, how to use the GUI

# F) Install Cape Without Docker (just in case)

**We just list this for completeness, you should not need to go though these steps!**.

Cape requires python 3 and uses python's [tkinter](https://docs.python.org/3/library/tkinter.html) for its graphical UI. For example, on ubuntu you can install the prerequisites with:

~~~shell
sudo apt-get install python3 python3-pip python3-tk
~~~


## Build and Install Cape

As mentioned before, Cape is written in Python. We recommend creating a python3 [virtual environment](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/). There are several ways to do that. Here we illustrate one. First enter the directory in which you cloned the capexplain git repository.

~~~shell
cd capexplain
~~~

Create the virtual environment.

~~~shell
python3 -m venv env
~~~

Activate the environment:

~~~shell
source ./env/bin/activate
~~~

Update `setuptools`:

~~~shell
python3 -m pip update setuptools
~~~

Install Cape:

~~~shell
python3 setup.py install
~~~

### Test the Installation

You can run

~~~shell
capexplain help
~~~

This should produce an output like this:

~~~shell
$capexplain help
capexplain COMMAND [OPTIONS]:
	explain unusually high or low aggregation query results.

AVAILABLE COMMANDS:

mine                          - Mining patterns that hold for a relation (necessary preprocessing step for generating explanations.
explain                       - Generate explanations for an aggregation result (patterns should have been mined upfront using mine).
stats                         - Extracting statistics from database collected during previous mining executions.
help                          - Show general or command specific help.
gui                           - Open the Cape graphical explanation explorer.
~~~

## Install Postgres + Load Database ##

We provide a docker image with a Postgres database that contains the datasets used in the experiments. If you do not have docker, please install it:

- on mac: [https://docs.docker.com/docker-for-mac/install/](https://docs.docker.com/docker-for-mac/install/)

here: First pull the image from dockerhub:

~~~sh
docker pull iitdbgroup/2019-sigmod-reproducibility-cape-postgres
~~~

You can create a container from this image using

~~~sh
docker run --rm --name cape-postgres -d -p 5436:5436 iitdbgroup/2019-sigmod-reproducibility-cape-postgres
~~~

To test the container you can connect to the database and test it using:

~~~shell
docker exec -ti cape-postgres psql -U antiprov -p 5436
psql (10.10)
Type "help" for help.

antiprov=#
~~~

This will connect to the Postgres instance using Postgres's commandline client `psql`. You can quit the client using `\q`.

## Run Experiments

All experiment scripts are in the `reproduce` folder. First enter with

~~~shell
cd reproduce
~~~

Then to reproduce all the experiment results you can simply run the script

~~~shell
./script.sh
~~~

The result will be in `experiments` folder (If you are using docker for cape, this folder will get copied to `docker` folder). In below subsections we will explain which file corresponds to which figure in the paper.

Alternatively you can run each part of the experiment separately.

### Pattern Mining

To run all the performance experiments of pattern mining,

~~~shell
./mining.sh
~~~

This will generate the needed csv files for plot in `experiments` folder. The python3 script and the makefile to plot the result is already provided. You just need to finish other experiments and run make. check for Plot below for detail.

### Explanation Generation

To test if the experiment environment has been setup correctly, we provide a script to run the performance evaluation experiment with few user questions. In `reproduce/expl_perf_exp` forlder, run

~~~shell
./perf_exp_crime_small.sh
~~~

to see if the code runs and plots `reproduce/expl_perf_exp/expl_crime_numpat.pdf` and `reproduce/expl_perf_exp/expl_crime_numatt.pdf` generated.

To reproduce the result, in `reproduce` folder:

~~~shell
./explanation.sh
~~~

The script `explanation.sh` will run all experiments for explanation generation; you can also run each experiment separately.

#### Performance
To run performance evaluation experiments separately, in `reproduce/expl_perf_exp`, run:

~~~shell
./perf_exp_crime.sh
./perf_exp_dblp.sh
~~~

#### Explanation Quality
To run quality evaluation experiments separately, in `reproduce/expl_qual_exp`, run:

~~~shell
./qual_exp_crime.sh
./qual_exp_dblp.sh
~~~

The results for Table 3 and Table 4 are in `reproduce/experiments/expl_qual_dblp.txt`, and the results for Table 5 are in `reproduce/experiments/expl_qual_crime.txt`.

#### Parameter Sensitivity:
To run parameter sensitivity evaluation experiments separately, in `reproduce/expl_param_exp`, run:

~~~shell
./params_exp.sh
~~~

## Plot results

If you ran all the experiments separately instead of using `script.sh`, then after all experiments are finished, enter `experiments` folder, run

~~~shell
make
~~~

And this will generate all plots.

Either way, the result of Figure 3 (a) is in `crime_num_att.pdf`; Figure 3 (b) is in `crime_size.pdf`;  Figure 3 (c) is in `dblp_size.pdf`; Figure 4 is in `crime_bar.pdf`; Figure 5 is in `crime_fd_on_off.pdf`; Figure 6 (a) is in `expl_DBLP_numpat.pdf`; Figure 6 (b) is in `expl_crime_numpat.pdf`; Figure 6 (c) is in `expl_crime_numatt.pdf`. Figure 7 is in `params_gs.pdf`.



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
