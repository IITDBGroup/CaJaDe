# 5 queries / questions from MIMIC

# Q1:
mimic_question_dict_1 = {}
mimic_uq1 = "provenance of (select 1.0*SUM(a.hospital_expire_flag)/count(*) as death_rate, count(*) as cnt, d.chapter from admissions a, diagnoses d where a.hadm_id=d.hadm_id group by d.chapter);"
mimic_question_dict_1['uquery'] = (mimic_uq1, 'mimic_qw_q1_diag_death_rate')
mimic_question_dict_1['question'] =["chapter='2'","chapter='13'"]
mimic_question_dict_1['uattrs'] = [('diagnoses','chapter'),('admissions','hospital_expire_flag'), ('admissions','insurance')]
mimic_question_dict_1['umap']= {'yes':'2', 'no':'13'}

# Q2:
mimic_question_dict_2 = {}
mimic_uq2 = "provenance of (select insurance, 1.0*SUM(hospital_expire_flag)/count(*) as death_rate from admissions group by insurance);"
mimic_question_dict_2['uquery'] = (mimic_uq2, 'mimic_qw_q2_medicaid_vs_medicare')
mimic_question_dict_2['question'] =["insurance='Medicaid'","insurance='Medicare'"]
mimic_question_dict_2['uattrs'] = [('admissions','insurance'),('admissions','hospital_expire_flag')]
mimic_question_dict_2['umap'] = {'yes':'Medicaid', 'no':'Medicare'}


# Q3:
mimic_question_dict_3 = {}
mimic_uq3 = "provenance of (select count(*) as cnt, los_group from icustays group by los_group);"
mimic_question_dict_3['uquery'] = (mimic_uq3, 'mimic_qw_q3_los')
mimic_question_dict_3['question'] =["los_group='x>8'","los_group='0-1'"]
mimic_question_dict_3['uattrs'] = [('icustays','los_group'), ('icustays','los')]
mimic_question_dict_3['umap'] = {'yes':'x>8', 'no':'0-1'}


mimic_question_dict_4 = {}
mimic_uq4 = "provenance of (select insurance, 1.0*SUM(hospital_expire_flag)/count(*) as death_rate from admissions group by insurance);"
mimic_question_dict_4['uquery'] = (mimic_uq2, 'mimic_qw_q2_gov_selfpay')
mimic_question_dict_4['question'] =["insurance='Medicare'","insurance='Private'"]
mimic_question_dict_4['uattrs'] = [('admissions','insurance'),('admissions','hospital_expire_flag')]
mimic_question_dict_4['umap'] = {'yes':'Medicare', 'no':'Private'}

# Q5
mimic_question_dict_5 = {}
mimic_uq5 = "provenance of (select count(*) as cnt, pai.ethnicity from patients_admit_info pai, procedures p where p.hadm_id=pai.hadm_id and p.subject_id = pai.subject_id group by pai.ethnicity);"
mimic_question_dict_5['uquery'] = (mimic_uq5, 'mimic_qw_q5_ethnicity_procdure')
mimic_question_dict_5['question'] =["ethnicity='ASIAN'","ethnicity='HISPANIC'"]
mimic_question_dict_5['uattrs'] = [('patients_admit_info','ethnicity')]
mimic_question_dict_5['umap']= {'yes':'ASIAN', 'no':'HISPANIC'}


mimic_cases = [mimic_question_dict_1,mimic_question_dict_2,mimic_question_dict_3, mimic_question_dict_5]



# 5 queries / questions from NBA

# Q1
nba_question_dict_1 = {}
nba_uq1 = "provenance of (select avg(points) as avp_pts, s.season_name from  player p, player_game_stats pgs, game g, season s where p.player_id=pgs.player_id and g.game_date = pgs.game_date and g.home_id = pgs.home_id and s.season_id = g.season_id and p.player_name='Draymond Green' group by season_name);"
nba_question_dict_1['uquery'] = (nba_uq1, 'nba_qw_q1_green_pts')
nba_question_dict_1['question'] =["season_name='2015-16'","season_name='2016-17'"]
nba_question_dict_1['uattrs'] = [('season','season_name'), ('player','player_name'), ('player_game_stats', 'points')]
nba_question_dict_1['umap']= {'yes':'2015-16', 'no':'2016-17'}

# # Q2
nba_question_dict_2 = {}
nba_uq2 = "provenance of (select avg(tgs.assists) as avgast, s.season_name from team_game_stats tgs, game g, team t, season s where s.season_id = g.season_id and tgs.game_date = g.game_date and tgs.home_id=g.home_id and tgs.team_id = t.team_id and t.team='GSW' group by s.season_name);"
nba_question_dict_2['uquery'] = (nba_uq2, 'nba_qw_q2_gsw_assists')
nba_question_dict_2['question'] =["season_name='2013-14'","season_name='2014-15'"]
nba_question_dict_2['uattrs'] = [('season','season_name'), ('team','team'), ('team_game_stats', 'assists')]
nba_question_dict_2['umap']= {'yes':'2013-14', 'no':'2014-15'}

# #Q3
nba_question_dict_3 = {}
nba_uq3 = "provenance of (select avg(points) as avp_pts, s.season_name from  player p, player_game_stats pgs, game g, season s where p.player_id=pgs.player_id and g.game_date = pgs.game_date and g.home_id = pgs.home_id and s.season_id = g.season_id and p.player_name='LeBron James' group by season_name);"
nba_question_dict_3['uquery'] = (nba_uq3, 'nba_qw_q3_lbj_pts')
nba_question_dict_3['question'] =["season_name='2009-10'","season_name='2010-11'"]
nba_question_dict_3['uattrs'] = [('season','season_name'), ('player','player_name'), ('player_game_stats', 'points')]
nba_question_dict_3['umap']= {'yes':'2009-10', 'no':'2010-11'}


#4
nba_question_dict_4 = {}
nba_uq4 = "provenance of (select count(*) as win, s.season_name from team t, game g, season s where t.team_id = g.winner_id and g.season_id = s.season_id and t.team= 'GSW' group by s.season_name);"
nba_question_dict_4['uquery'] = (nba_uq4,'nba_qw_q4_gsw')
nba_question_dict_4['question'] =["season_name='2016-17'","season_name='2012-13'"]
nba_question_dict_4['uattrs'] = [('season','season_name'), ('team','team')]
nba_question_dict_4['umap']= {'yes':'2016-17', 'no':'2012-13'}


#Q5
nba_question_dict_5 = {}
nba_uq5 = "provenance of (select avg(points) as avp_pts, s.season_name from  player p, player_game_stats pgs, game g, season s where p.player_id=pgs.player_id and g.game_date = pgs.game_date and g.home_id = pgs.home_id and s.season_id = g.season_id and p.player_name='Jimmy Butler' group by season_name);"
nba_question_dict_5['uquery'] = (nba_uq5, 'nba_qw_q5_butler_pts')
nba_question_dict_5['question'] =["season_name='2014-15'","season_name='2013-14'"]
nba_question_dict_5['uattrs'] = [('season','season_name'), ('player','player_name'), ('player_game_stats', 'points')]
nba_question_dict_5['umap']= {'yes':'2014-15', 'no':'2013-14'}

#Q6
nba_question_dict_6 = {}
nba_uq6 = "provenance of (select round(avg(pgs.points),2) as avg_pts, g.game_month from player_game_stats pgs, game g, player p where g.game_date=pgs.game_date and g.home_id=pgs.home_id and pgs.player_id = p.player_id and g.game_year='2013' and p.player_name='Shane Battier' group by game_month);"
nba_question_dict_6['uquery'] = (nba_uq6, 'nba_qw_q6_battier_pts')
nba_question_dict_6['question'] =["game_month='1'","game_month='2'"]
nba_question_dict_6['uattrs'] = [('player','player_name'), ('player_game_stats', 'points'), ('game', 'game_month')]
nba_question_dict_6['umap']= {'yes':'1', 'no':'2'}

nba_cases = [nba_question_dict_1,nba_question_dict_2, nba_question_dict_3, nba_question_dict_4,nba_question_dict_5]
