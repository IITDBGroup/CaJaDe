#!/bin/bash
sudo docker build --no-cache -f ./Dockerfile -t jayli2018/2021-sigmod-reproducibility-cajade_systems:latest .
sudo docker push jayli2018/2021-sigmod-reproducibility-cajade_systems:latest