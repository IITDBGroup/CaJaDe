#!/bin/bash
sudo docker build --no-cache -f ./Dockerfile -t jayli2018/2021-sigmod-reproducibility-cajade_postgres .
sudo docker run -it -p 5432:5432 -e POSTGRES_PASSWORD=reproduce  jayli2018/2021-sigmod-reproducibility-cajade_postgres:latest postgres
# need to run this to load data
# sudo docker push jayli2018/2021-sigmod-reproducibility-cajade_postgres:latest