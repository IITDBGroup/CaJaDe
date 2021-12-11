########################################
# Derived from python image
########################################
FROM python:3.6.15
MAINTAINER Chenjie Li <cli112@hawk.iit.edu>
########################################
# Install packages needed for building
########################################
RUN apt-get -y update && \
    apt-get -y --no-install-recommends install python3-tk  &&  \
    rm -rf /var/lib/apt/lists/*
########################################
# Clone Cape repository
########################################
ADD experiments_nba.sh player_q.txt team_q.txt ./
RUN git clone --single-branch --branch master https://github.com/IITDBGroup/cape.git 
########################################
# Install cape
########################################
RUN cd cape && \
	python3 setup.py install