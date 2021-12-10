########################################
# Derived from python image
########################################
FROM python:2.7.18
MAINTAINER Chenjie Li <cli112@hawk.iit.edu>

########################################
# Install packages needed for building
########################################
RUN mkdir /ExplanationTable

########################################
ADD explanationTables /ExplanationTable

########################################
# Install needed packages
########################################
RUN cd /ExplanationTable \
&& pip install -r requirements.txt