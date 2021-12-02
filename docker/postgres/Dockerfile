########################################
# Derived from postgres container
########################################

FROM postgres:10.14
MAINTAINER Chenjie Li <cli112@hawk.iit.edu>

RUN mkdir /dumpfile
ADD init_dbs.sql load_dumps.sh /docker-entrypoint-initdb.d/
ADD reproduce_nba01.sql  reproduce_nba05.sql  reproduce_nba1.sql  reproduce_nba2.sql  reproduce_nba4.sql reproduce_nba8.sql /dumpfile/
