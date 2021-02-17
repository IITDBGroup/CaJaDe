# 5 queries / questions from MIMIC

# Q1:

mimic_question_dict_1 = {}
mimic_uq1 = "provenance of (select count(*) as cnt, chapter from diagnoses group by chapter);"
mimic_question_dict_1['uquery'] = (mimic_uq1, 'mimic_qw_q1_chapter_7_vs_chapter_11')
mimic_question_dict_1['question'] =["chapter='7'","chapter='11'"]
mimic_question_dict_1['uattrs'] = [('diagnoses','chapter')]
mimic_question_dict_1['umap']= {'yes':'7', 'no':'11'}

# --  count  | chapter 
# -- --------+---------
# --   20264 | 1
# --      47 | -1
# --   32867 | 10
# --     654 | 11 <- 
# --    8789 | 12 
# --   13392 | 13
# --    4078 | 14
# --   20160 | 15
# --   29691 | 16
# --   42948 | 17
# --   14235 | 2
# --   69592 | 3
# --   25262 | 4
# --   24876 | 5
# --   23334 | 6
# --  140257 | 7 <-
# --   44825 | 8
# --   38527 | 9
# --   22544 | E
# --   74705 | V


# Q2:
mimic_question_dict_2 = {}
mimic_uq2 = "provenance of (select insurance, 1.0*SUM(hospital_expire_flag)/count(*) as death_rate from admissions group by insurance);"
mimic_question_dict_2['uquery'] = (mimic_uq2, 'mimic_qw_q2_selfpay_vs_private')
mimic_question_dict_2['question'] =["insurance='Self Pay'","insurance='Private'"]
mimic_question_dict_2['uattrs'] = [('admissions','insurance')]
mimic_question_dict_2['umap'] = {'yes':'Self Pay', 'no':'Private'}

# -- insurance  |       death_rate       
# -- ------------+------------------------
# -- Government | 0.05047672462142456534
# -- Self Pay   | 0.15548281505728314239 <-
# -- Medicare   | 0.13840155945419103314 
# -- Private    | 0.06124346824904791427 <-
# -- Medicaid   | 0.06585998271391529818


# Q3:
mimic_question_dict_3 = {}
mimic_uq3 = "provenance of (select count(*) as cnt, los_group from icustays group by los_group);"
mimic_question_dict_3['uquery'] = (mimic_uq3, 'mimic_qw_q3_los')
mimic_question_dict_3['question'] =["los_group='x>8'","los_group='0-1'"]
mimic_question_dict_3['uattrs'] = [('icustays','los_group')]
mimic_question_dict_3['umap'] = {'yes':'x>8', 'no':'0-1'}

#  count | los_group 
# -------+-----------
#  16901 | 1-2
#   8605 | x>8  <-
#  15034 | 2-4
#  12311 | 0-1  <-
#   8671 | 4-8


# Q4:
mimic_question_dict_4 = {}
mimic_uq4 = "provenance of (select count(*) as cnt, chapter from procedures group by chapter);"
mimic_question_dict_4['uquery'] = (mimic_uq4, 'mimic_qw_q4_procedures')
mimic_question_dict_4['question'] =["chapter='16'","chapter='14'"]
mimic_question_dict_4['uattrs'] = [('procedures','chapter')]
mimic_question_dict_4['umap']= {'yes':'16', 'no':'14'}

# -------+---------
#     37 | 3A
#     64 | 4
#     88 | 13
#    219 | 2
#    240 | 3
#    467 | 12
#    965 | 5
#   1445 | 8
#   1696 | 10
#   2291 | 11
#   3836 | 15
#   7600 | 1
#   8283 | 0
#  10184 | 14 <-
#  15373 | 6
#  23784 | 9
#  69996 | 7
#  93527 | 16 <-



# Q5
mimic_question_dict_5 = {}
mimic_uq5 = "provenance of (select count(*) as cnt, ethnicity from patients_admit_info group by ethnicity);"
mimic_question_dict_5['uquery'] = (mimic_uq5, 'mimic_qw_q5_ethnicity')
mimic_question_dict_5['question'] =["ethnicity='ASIAN'","ethnicity='HISPANIC OR LATINO'"]
mimic_question_dict_5['uattrs'] = [('patients_admit_info','ethnicity')]
mimic_question_dict_5['umap']= {'yes':'ASIAN', 'no':'HISPANIC OR LATINO'}

#   cnt  |                        ethnicity
# -------+----------------------------------------------------------
#      3 | AMERICAN INDIAN/ALASKA NATIVE FEDERALLY RECOGNIZED TRIBE
#      4 | HISPANIC/LATINO - HONDURAN
#      4 | ASIAN - THAI
#      7 | ASIAN - JAPANESE
#      8 | SOUTH AMERICAN
#      9 | CARIBBEAN ISLAND
#      9 | HISPANIC/LATINO - COLOMBIAN
#     13 | HISPANIC/LATINO - CENTRAL AMERICAN (OTHER)
#     13 | HISPANIC/LATINO - MEXICAN
#     13 | ASIAN - KOREAN
#     17 | ASIAN - OTHER
#     17 | ASIAN - CAMBODIAN
#     18 | NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER
#     19 | HISPANIC/LATINO - SALVADORAN
#     24 | HISPANIC/LATINO - CUBAN
#     25 | ASIAN - FILIPINO
#     25 | WHITE - EASTERN EUROPEAN
#     40 | HISPANIC/LATINO - GUATEMALAN
#     43 | MIDDLE EASTERN
#     44 | BLACK/AFRICAN
#     51 | AMERICAN INDIAN/ALASKA NATIVE
#     53 | ASIAN - VIETNAMESE
#     59 | WHITE - BRAZILIAN
#     61 | PORTUGUESE
#     78 | HISPANIC/LATINO - DOMINICAN
#     81 | WHITE - OTHER EUROPEAN
#     85 | ASIAN - ASIAN INDIAN
#    101 | BLACK/HAITIAN
#    130 | MULTI RACE ETHNICITY
#    164 | WHITE - RUSSIAN
#    200 | BLACK/CAPE VERDEAN
#    232 | HISPANIC/LATINO - PUERTO RICAN
#    277 | ASIAN - CHINESE
#    559 | PATIENT DECLINED TO ANSWER
#    814 | UNABLE TO OBTAIN
#   1509 | ASIAN                        <-
#   1512 | OTHER
#   1696 | HISPANIC OR LATINO           <-
#   4523 | UNKNOWN/NOT SPECIFIED
#   5440 | BLACK/AFRICAN AMERICAN
#  40996 | WHITE


mimic_workloads = [mimic_question_dict_1,mimic_question_dict_2,mimic_question_dict_3,mimic_question_dict_4, mimic_question_dict_5]

# print(mimic_workloads)
# 5 queries / questions from NBA


# Q1
nba_question_dict_1 = {}
nba_uq1 = "provenance of (select avg(points), season_name from player natural join player_game_stats natural join game natural join season where player_name='Draymond Green' group by season_name);"
nba_question_dict_1['uquery'] = (nbaq1, 'nba_qw_q1_green_pts')
nba_question_dict_1['question'] =["season_name='2015-16'","season_name='2016-17'"]
nba_question_dict_1['uattrs'] = [('season','season_name'), ('player','player_name')]
nba_question_dict_1['umap']= {'yes':'2015-16', 'no':'2016-17'}

nba_workloads = [nba_question_dict_1]

# # Q2
# nba_question_dict_2 = {}
# nba_uq2 = "provenance of (select count(*) as cnt, ethnicity from patients_admit_info group by ethnicity);"
# nba_question_dict_2['uquery'] = (nbaq2, 'nba_qw_q2_ethnicity')
# nba_question_dict_2['question'] =["ethnicity='ASIAN'","ethnicity='HISPANIC OR LATINO'"]
# nba_question_dict_2['uattrs'] = [('patients_admit_info','ethnicity')]
# nba_question_dict_2['umap']= {'yes':'ASIAN', 'no':'HISPANIC OR LATINO'}


# #Q3
# nba_question_dict_3 = {}
# nba_uq3 = "provenance of (select count(*) as cnt, ethnicity from patients_admit_info group by ethnicity);"
# nba_question_dict_3['uquery'] = (nbaq3, 'nba_qw_q3_ethnicity')
# nba_question_dict_3['question'] =["ethnicity='ASIAN'","ethnicity='HISPANIC OR LATINO'"]
# nba_question_dict_3['uattrs'] = [('patients_admit_info','ethnicity')]
# nba_question_dict_3['umap']= {'yes':'ASIAN', 'no':'HISPANIC OR LATINO'}


# #4
# nba_question_dict_3 = {}
# nba_u4 = "provenance of (select count(*) as cnt, ethnicity from patients_admit_info group by ethnicity);"
# nba_question_dict_3['uquery'] = (nba4, 'nba_qw_4_ethnicity')
# nba_question_dict_3['question'] =["ethnicity='ASIAN'","ethnicity='HISPANIC OR LATINO'"]
# nba_question_dict_3['uattrs'] = [('patients_admit_info','ethnicity')]
# nba_question_dict_3['umap']= {'yes':'ASIAN', 'no':'HISPANIC OR LATINO'}


# #Q5
# nba_question_dict_3 = {}
# nba_uq3 = "provenance of (select count(*) as cnt, ethnicity from patients_admit_info group by ethnicity);"
# nba_question_dict_3['uquery'] = (nbaq3, 'nba_qw_q3_ethnicity')
# nba_question_dict_3['question'] =["ethnicity='ASIAN'","ethnicity='HISPANIC OR LATINO'"]
# nba_question_dict_3['uattrs'] = [('patients_admit_info','ethnicity')]
# nba_question_dict_3['umap']= {'yes':'ASIAN', 'no':'HISPANIC OR LATINO'}

