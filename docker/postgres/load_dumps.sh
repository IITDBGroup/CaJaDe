#!/bin/bash

psql -p 5432 -U cajade -d nba01 < ../dumpfile/reproduce_nba01.sql
psql -p 5432 -U cajade -d nba05 < ../dumpfile/reproduce_nba05.sql
psql -p 5432 -U cajade -d nba < ../dumpfile/reproduce_nba1.sql
psql -p 5432 -U cajade -d nba2 < ../dumpfile/reproduce_nba2.sql
psql -p 5432 -U cajade -d nba4  < ../dumpfile/reproduce_nba4.sql
psql -p 5432 -U cajade -d nba8  < ../dumpfile/reproduce_nba8.sql
