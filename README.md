# SIGMOD Reproducibility for Paper "Putting Things into Context: Rich Explanations for Query Answers using Join Graphs"

[https://github.com/IITDBGroup/CaJaDe/tree/sigmod_reproducibility](https://github.com/IITDBGroup/CaJaDe/tree/sigmod_reproducibility)

Note: If you are viewing the PDF file, we added some additional notes in this README.


# A) Source Code Info

The **CaJaDE** system is written in `Python` and uses [**PostgreSQL**](https://www.postgresql.org/) as a backend for storage.  We use [**GProM**](https://github.com/IITDBGroup/gprom/tree/gprom_in_cajade) as the tool to generate the provenance of the query. 

As we have some comparison experiments with 2 previous work. we provide the access to those work as well: 

- For **CAPE** ([paper link](https://dl.acm.org/doi/10.1145/3299869.3300066])), you could access cape repository from [here](https://github.com/IITDBGroup/cape) `master` branch
- For **Explanation Table** ([paper link](https://dl.acm.org/doi/10.14778/2735461.2735467])) has no public repo, but we contacted the authors and got the source code. we include the code in a docker image which we discuss in detail in **Section D)**

The **CaJaDE** package installs a library as well as a commandline tool `cajadexplain`. This tool provides the arguments needed to run a query in a given database, with provided "2 point" user question, and some parameters to produce the explanations and save the explanations in the given database schema

- Repository: https://github.com/IITDBGroup/cajade (branch `SIGMOD-reproducibility`)
- Programming Language: Python
- Additional Programming Language info: we are requiring Python3. Specifically, we are using Python 3.8.10 in this reproducibility submission.
- OS for Experiments: Linux (required for docker)
- To summarize, in order to reproduce all the experiment results the following source codes are required :
    - [**GProM**](https://github.com/IITDBGroup/gprom/tree/gprom_in_cajade) 
    - [**CAPE**](https://github.com/IITDBGroup/cape)
    - [**CaJaDE**](https://github.com/IITDBGroup/CaJaDe/tree/sigmod_reproducibility)
    -  [**PostgreSQL**](https://www.postgresql.org/) 

# B) Datasets

- NBA dataset: NBA (National Basketball Association) dataset were extracted from [this cite](http://www.pbpstats.com/). This dataset will be available in the reproducibility repo
- MIMIC dataset:  in order to access MIMIC (Medical Information Mart for Intensive Care) dataset, it requires user to finish the steps listed [here](https://mimic.mit.edu/docs/gettingstarted/) before starting working with the data. We do not provide the dataset and the experiment scripts in this initial submission due to the policy. However, we encourage reviewers to ask for permission from [here](https://mimic.mit.edu/docs/gettingstarted/) . Please contact us once you get the access to the dataset and we will provide the remaining dataset/scripts needed to reproduce the experiments related to MIMIC dataset. 

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

Please clone the CaJaDE git repository and check out the `sigmod-reproducibility` branch. This branch contains Cape as well as scripts for running experiments and plotting results.

~~~shell
git clone --single-branch --branch sigmod_reproducibility https://github.com/IITDBGroup/CaJaDe.git
~~~

To check whether it was cloned correctly run:

~~~shell
cd CaJaDe
tree -d
~~~

this should produce an output like this:

~~~shell
.
├── docker
│   ├── cajade
│   ├── cape
│   ├── et
│   │   └── explanationTables
│   └── postgres
├── reproduce
└── src

~~~

## Start-up Docker Cluster

We are using `docker-compose` for this. Switch to the `docker` directory inside the git repository you have cloned.

~~~shell
cd docker
~~~

The following commands may require `root`

Now run docker-compose which will create a `cape-system` and a `postgres-cape` container:

~~~shell
docker-compose up -d
~~~

Using `-d` flag will set up the containers in [detached mode](https://docs.docker.com/compose/reference/up/)

**NOTE**: since the loading of the dataset in postgres needs a bit of time, so we recommend use non detached mode in one terminal so that you could see the process of loading data has finished in the output log:

You could start running experiments when the log shows the following:

```she
LOG:  database system is ready to accept connections
```

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

Due to the complexity to put every piece together, we have made several [docker](https://www.docker.com/) images for the reviewers' convenience to reproduce the experiments' results.

There are several scripts from 3 of the containers driving the experiments. These scripts copies results to the directory from which you ran `docker-compose`.  The full set of experiments in this submission will run roughly for no more than 10 hours.  So you could finish it at once by following the instructions listed below.

### *1. Container `cajade-main` :  main experiments on CaJaDE*

Once you set docker containers up, you could start by running the main experiments using the following command

To start the experiments:

~~~shell
docker exec -ti cajade-main /CaJaDe/reproduce/experiments_nba.sh
~~~

Over time you should see CSV and PDF files pop up in the `docker` folder in the cloned git repository.

script `experiments_nba.sh` will run the following experiments

In thus container we evaluated the following things:

- scalability on different sizes of dataset (**Figure** 7 in the paper)
- performance on different workloads (**Figure** 10)
- repeat case study queries (**Figure** 5,  **Figure** 6)
- LCA sampling effects (**Figure** 8(a) - 8(d))
- F1 sample effects on runtime and quality (**Figure** 8(f))
- Runtime comparison with Explanation Table

Please note some of the graphs listed above belong to MIMIC dataset and we need reviewer get access to the dataset before providing the related data and scripts (see Section B)

### *2. Container `cajade-cape` :  explanations generated by CAPE on similar questions*

We provided 2 questions from NBA dataset (question details please refer to our paper) on CAPE system and demonstrate the weakness of CAPE in generating valuable explanations when the interesting data is not part of the given dataset. (**Figure** 11)

All generated result files will be in `docker` folder within the CaJaDe repository on your local machine. 

To run the experiment on this container, run:

```she
docker exec -ti cajade-cape /experiments_nba.sh
```

The top explanations generated by CAPE will show up after the shell script finishes.

### 3. Container  `cajade-et`:  runtime comparison with Explanation Table

We use this container to reproduce **Figure** 9. Specifically, we wanted to compare the runtime performance between CaJaDE and Explanation Table on a given materialized join graph result. This container will generate the runtime for Explanation Table and the runtime from CaJaDE will come from the shell script from `cajade-main`. 

To run the experiment on this container, run:

```she
docker exec -ti cajade-et /ExplanationTable/run_et_on_nba.sh
```



# E) Suggestions and Instructions for Alternative Experiments

For convenience, we provide the single script that runs all experiments. However, CaJaDE could run any given 2 point user questions that can be passed to it from command line arguments.

To interact with ``CaJaDE` `` command line tool, you could execute the following command using docker 

```she
docker exec -ti cajade-main /bin/bash
```

And you will be inside the container that has every component set up for you to run your query on NBA/MIMIC dataset.

You could type in the following command to see the available options when running `cajadexplain`

```she
usage: cajadexplain [-h] [-M] [-Q] [-A] [-F] [-w] [-z] [-o] [-i] [-m] [-r] [-s] [-S] [-H] [-P] [-D] [-t] [-W] [-C] [-L] -U -p -d 

Running experiments of CaJaDe

optional arguments:
  -h, --help            show this help message and exit
  -M, --maximum_edges 
                        Maximum number of edges allowed in a join graph (default: 3)
  -Q, --user_query  User query: default(select count(*) as win, s.season_name from team t, game g, season s where t.team_id = g.winner_id and g.season_id = s.season_id and t.team= 'GSW' group by s.season_name)
  
  -A, --user_question 
                        User question: need to strictly follow the output format of query result and use | as delimiter default(season_name='2015-16'|season_name='2012-13')
  -F, --f1_sample_rate 
                        Sample rate of apt when calculating the f1 score (default: 0.3)
  -w, --f1_sample_type 
                        Sample type of apt when calculating the f1 score (default: weighted)
  -z, --f1_sample_thresh 
                        Sample threshold of APT, only sample if apt bigger than this (default: 100)
  -o, --optimized   use opt or not (y: yes, n: no), (default: y)
  -i, --ignore_expensive 
                        skip expensive jg or not, (default: true)
  -m, --min_recall_threshold 
                        recall threshold (default: 0.3)
  -r, --sample_rate_for_lca 
                        sample rate for lca (default: 0.05)
  -s, --min_lca_s_size 
                        min size of sample used for lca cross product (default: 100)
  -S, --max_lca_s_size 
                        min size of sample used for lca cross product (default: 1000)
  -H, --db_host     database host, (default: localhost)
  -P, --port        database port, (default: 5432)
  -D, --result_schema 
                        result_schema_name_prefix, (default: exp_[timestamp of the start]
  -t, --f1_calc_type 
                        f1 score type (s sample, o original, e: evaluate_sample) (default: s)
  -W, --workloads   using questions from workloads? (default: false)
  -C, --case_studies 
                        using questions from case studies? (default: false)
  -L, --evaluate_lca_mode 
                        generate LCA results only, will not generate pattern results, (default: false)

required named arguments:
  -U, --user_name   owner of the database (required)
  -p, --password    password to the database (required)
  -d, --db_name     database name (required)

```

As shown above,  you could freely change any parameter as you like and produce explanations.

For your convenience to get started, we provide a template command below for you to run on NBA dataset.

```shell
 cajadexplain -H 10.5.0.3 -M 3 -p reproduce -U cajade -P 5432 -d nba -t s  -D nba_schema
```

Then you should be able to see a log information about the parameters you specified or the default values. Then after a few minutes the command program should finish itself!

After that, you could actually query to find what kind of patterns are generated.

Since postgres database is located in another container `cajade-psql` connected with this container. So you would need to go to that container and see the results. 

To do that, run 

```shel
sudo docker exec -it cajade-psql /bin/bash
```

Then you will be in the container that has postgres which has the patterns generated before.  To log into `psql` and write queries on database `nba` run:

```shell
psql -U cajade -d nba
```

After this, to retrieve `global patterns` which is a topk patterns from each join graph generated. run 

```sql
SELECT p_desc, jg_details, recall, precision, fscore, is_user FROM nba_schema.global_results;
```

In this query 

- `p_desc` : pattern description

- `jg_details`: a text description for the join graph that generates the pattern

- `recall, precision, fscore` are the standard metrics discussed in the paper

- `is_user`: a text describes the `primary tuple`(see paper for definition) that is evaluated when calculating the scores

  

# F) Links and Contact Information #

CaJaDe is developed by researchers at Illinois Institute of Technology and Duke University. 

For questions about the experiments or CaJaDE feel free to contact us:

- **Chenjie Li** [cli112@hawk.iit.edu](cli112@hawk.iit.edu)
- **Zhengjie Miao** [zhengjie.miao@duke.edu](zhengjie.miao@duke.edu)
