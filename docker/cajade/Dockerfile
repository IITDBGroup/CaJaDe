########################################
# Derived from Ubuntu image
########################################
FROM ubuntu:20.04
MAINTAINER Chenjie Li <cli112@hawk.iit.edu>

########################################
# Setup GProM required prerequisites
########################################
ENV DEBIAN_FRONTEND=noninteractive 

RUN apt update \
&& apt install -y git \
&& apt-get install -y make autoconf libtool libreadline6-dev libpq-dev bison flex 

########################################
# Clone GProM github repository
########################################
RUN git clone --single-branch --branch gprom_in_cajade https://github.com/IITDBGroup/gprom.git 

########################################
# Build GProM
########################################
RUN cd /gprom \
	&& ./autogen.sh \
	&& CFLAGS="-Wno-misleading-indentation -I/usr/include/postgresql/" ./configure --disable-oracle --disable-monetdb --disable-java --disable-sqlite \
	&& make \
	&& make install

########################################
# Setup CaJaDE required prerequisites
########################################
RUN apt-get install --assume-yes software-properties-common \
&& add-apt-repository -y ppa:deadsnakes/ppa \
&& apt-get install -y python3.6 python3-virtualenv 

########################################
# Clone Cajade and install
########################################
RUN git clone --single-branch --branch sigmod_reproducibility https://github.com/IITDBGroup/CaJaDe.git \
&& apt-get install python3-setuptools

RUN cd /CaJaDe \
&& python3.6 setup.py install \
