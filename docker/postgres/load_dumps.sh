#!/bin/bash

psql -p 5432 -U cajade -d nba01 < ../datafiles/reproduce_nba01.sql
psql -p 5432 -U cajade -d nba05 < ../datafiles/reproduce_nba05.sql
psql -p 5432 -U cajade -d nba < ../datafiles/reproduce_nba1.sql
psql -p 5432 -U cajade -d nba2 < ../datafiles/reproduce_nba2.sql
psql -p 5432 -U cajade -d nba4  < ../datafiles/reproduce_nba4.sql
psql -p 5432 -U cajade -d nba8  < ../datafiles/reproduce_nba8.sql
psql -p 5432 -U cajade -d nba_cape  < ../datafiles/reproduce_cape.sql
psql -p 5432 -U cajade -d nba_et  < ../datafiles/reproduce_et.sql
