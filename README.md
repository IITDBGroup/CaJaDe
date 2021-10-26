# **CaJaDe**
This is the branch used to submit to  [SIGMOD 2021 Reproducibility](https://reproducibility.sigmod.org/)

TODOs:

- Set up docker environment
  - Linux 
  - GProm
  - Python with same package versions
  - Postgres with same version on Debussy
- Prepare datasets
  - NBA: just directly dump from Debussy
  - MIMIC: give instructions and scripts to prepare dataset due to restriction on access
- Experiments:
  - 2 Scalability graphs : NBA + MIMIC
  - 4 Graphs on LCA sample size: NBA only
  - 2 Graphs Sampling effects on runtime and quality: NBA +MIMIC
  - Query workloads 5 NBA + 5 MIMIC
  - 2 Comparisons
    - CAPE (what to do about this given some packages may require different verisons?)
    -  Explanation tables
- Case Study and User Study: 
  - NBA and MIMIC
  - Do we need to do anything about user study?

## 1. Datasets

- NBA dataset: NBA (National Basketball Association) dataset were extracted from [this cite][http://www.pbpstats.com/]. This dataset will be available in the reproducibility repo
- MIMIC dataset:  in order to access MIMIC (Medical Information Mart for Intensive Care) dataset, it requires user to finish the steps listed [here][https://mimic.mit.edu/docs/gettingstarted/] before starting working with the data. We do not provide the dataset in this reproducibility repository due to the policy. However, we provide the scripts needed to prepare the processed MIMIC dataset used in the experiments.



## **2. System Specs**

All experiments were executed on a server with the following specs:

| Element          | Description                                                  |
| ---------------- | ------------------------------------------------------------ |
| CPU              | 2 x AMD Opteron(tm) Processor 4238, 3.3Ghz                   |
| Caches (per CPU) | L1 (288KiB), L2(6 MiB), L3(6 MiB)                            |
| Memory           | 128GB (DDR3 1333MHz)                                         |
| RAID Controller  | LSI Logic / Symbios Logic MegaRAID SAS 2108 [Liberator] (rev 05), 512MB cache |
| RAID Config      | 4 x 1TB, configured as RAID 5                                |
| Disks            | 4 x 1TB 7.2K RPM Near-Line SAS 6Gbps (DELL CONSTELLATION ES.3) |

## 
