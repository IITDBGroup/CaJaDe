#!/bin/bash

echo "CAPE: mine patterns from table player_game_stats ... "
capexplain mine -h 10.5.0.3 -u cajade -p reproduce -d nba_cape -P 5432 -t player_game_stats --numeric minutes,offposs,points,fg_two_a,fg_two_pct,fg_three_a,fg_three_pct,usage,assists,rebounds,defrebounds,offrebounds,season --summable minutes,offposs,points,fg_two_a fg_two_pct,fg_three_a,fg_three_pct,usage,assists,rebounds,defrebounds,offrebounds

echo "CAPE: mine patterns from table team_game_stats ... "
capexplain mine -h 10.5.0.3 -u cajade -p reproduce -d nba_cape -P 5432 -t team_game_stats --numeric points,fg_two_a,fg_two_pct,fg_three_a,fg_three_pct,assists,rebounds,defrebounds,offrebounds,season,win --summable points,fg_two_a,fg_two_pct,fg_three_a,fg_three_pct,assists,rebounds,defrebounds,offrebounds,win

echo "CAPE: explain player Lebron James Points low in 2010 ... "
capexplain explain -h 10.5.0.3  -u cajade -p reproduce -d nba_cape -P 5432 --qtable player_game_stats --ptable pattern.player_game_stats --ufile player_q.txt --ofile ./experiment_results/player_exps.txt 

echo "CAPE: explain team GSW wins low in 2012 ... "
capexplain explain -h 10.5.0.3  -u cajade -p reproduce -d nba_cape -P 5432 --qtable team_game_stats --ptable pattern.team_game_stats --ufile team_q.txt --ofile ./experiment_results/player_exps.txt 
