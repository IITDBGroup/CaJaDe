
# 1) MIMIC 
# psql -U japerev -d mimic_et 

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

table_name1 = 'jg_1'

label1 = 'is_user'

renmame_dict1 = {
'a_2': 'prov_admissions_admittime', 'a_3': 'prov_admissions_dischtime', 'a_4': 'prov_admissions_deathtime', 'a_5': 'prov_admissions_admission__type',
  'a_6': 'prov_admissions_admission__location', 'a_7': 'prov_admissions_discharge__location', 'a_9': 'prov_admissions_marital__status',
   'a_10': 'prov_admissions_edregtime', 'a_11': 'prov_admissions_edouttime', 'a_12': 'prov_admissions_diagnosis', 'a_13': 'prov_admissions_hospital__expire__flag'
}

considered_attrs_1:['a_2', 'a_3', 'a_4', 'a_5', 'a_6', 'a_7', 'a_9', 'a_10', 'a_11', 'a_12', 'a_13']

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

table_name2 = 'jg_10'

label2 = 'is_user'

renmame_dict2 = {'a_2': 'prov_admissions_admittime', 
'a_3': 'prov_admissions_dischtime', 'a_4': 'prov_admissions_deathtime', 'a_5': 'prov_admissions_admission__type', 
'a_6': 'prov_admissions_admission__location', 
'a_7': 'prov_admissions_discharge__location', 'a_9': 'prov_admissions_marital__status', 'a_10': 'prov_admissions_edregtime', 
'a_11': 'prov_admissions_edouttime', 'a_12': 'prov_admissions_diagnosis', 
'a_13': 'prov_admissions_hospital__expire__flag', 'a_15': 'ethnicity', 'a_16': 'religion', 'a_17': 'language', 'a_18': 'age', 
'a_21': 'expire_flag',  'a_23': 'gender', 'a_24': 'dob', 'a_25': 'dod', 'a_26': 'dod_hosp', 'a_27': 'dod_ssn'}

considered_attrs_2:['a_2', 'a_3', 'a_4', 'a_5', 'a_6', 'a_7', 'a_9', 'a_10', 'a_11', 'a_12', 'a_13', 'a_15', 
'a_16', 'a_17', 'a_18', 'a_21', 'a_23', 'a_24', 'a_25', 'a_26', 'a_27']

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# 2) NBA
# psql -U japerev -d nba_et

table_name3 = 'jg_288'

label3 = 'is_user'
 
renmame_dict3= {'a_1': 'prov_team_team__id', 'a_2': 'prov_team_team', 
'a_3': 'prov_game_game__date', 'a_4': 'prov_game_home__points', 'a_5': 'prov_game_away__points', 'a_6': 'prov_game_home__possessions', 
'a_7': 'prov_game_away__possessions', 'a_8': 'prov_game_home__id', 'a_9': 'prov_game_away__id', 'a_10': 'prov_game_winner__id', 
'a_11': 'prov_game_season__id', 'a_12': 'prov_season_season__id', 'a_13': 'prov_season_season__name', 'a_14': 'prov_season_season__type',
'a_15': 'def_three_ptrebounds', 'a_16': 'def_two_ptreboundpct', 'a_17': 'def_two_ptrebounds', 'a_18': 'defftreboundpct', 'a_19': 'ftdefrebounds', 
'a_20': 'defrebounds', 'a_21': 'rebounds', 'a_22': 'arc_three_assists', 'a_23': 'corner_three_assists', 'a_24': 'longmidrangeassists', 
'a_25': 'shortmidrangeassists', 'a_26': 'atrimassists', 'a_27': 'three_ptassists', 'a_28': 'two_ptassists', 'a_29': 'assistpoints',
 'a_30': 'assists', 'a_31': 'player_id', 'a_32': 'usage', 'a_33': 'fg_three_apctblocked', 'a_34': 'fg_three_ablocked', 'a_35': 'fg_two_apctblocked', 
 'a_36': 'fg_two_ablocked', 'a_37': 'ptsputbacks', 'a_38': 'tspct', 'a_39': 'efgpct', 'a_40': 'shotqualityavg', 'a_41': 'fg_three_apct', 
 'a_42': 'assisted_three_spct', 'a_43': 'nonputbacksassisted_two_spct', 'a_44': 'assisted_two_spct', 'a_45': 'ptsunassisted_three_s', 
 'a_46': 'ptsunassisted_two_s', 'a_47': 'ptsassisted_two_s', 'a_48': 'ftpoints', 'a_49': 'nonheavefg_three_pct', 'a_50': 'fg_three_pct', 
 'a_51': 'fg_three_a', 'a_52': 'fg_three_m', 'a_53': 'fg_two_pct', 'a_54': 'fg_two_a', 'a_55': 'fg_two_m', 'a_56': 'points', 'a_57': 'offposs', 
 'a_58': 'minutes', 'a_59': 'home_id', 'a_60': 'game_date', 'a_61': 'ptsassisted_three_s', 'a_62': 'offcorner_three_reboundpct', 'a_63': 'offarc_three_reboundpct',
  'a_64': 'offlongmidrangereboundpct', 'a_65': 'offshortmidrangereboundpct', 'a_66': 'offatrimreboundpct', 'a_67': 'defcorner_three_reboundpct', 
  'a_68': 'defarc_three_reboundpct', 'a_69': 'deflongmidrangereboundpct', 'a_70': 'defshortmidrangereboundpct', 'a_71': 'defatrimreboundpct', 'a_72': 'offfgreboundpct', 
  'a_73': 'off_three_ptreboundpct', 'a_74': 'off_three_ptrebounds', 'a_75': 'off_two_ptreboundpct', 'a_76': 'off_two_ptrebounds', 'a_77': 'offftreboundpct', 
  'a_78': 'ftoffrebounds', 'a_79': 'offrebounds', 'a_80': 'deffgreboundpct', 'a_81': 'def_three_ptreboundpct'}


considered_attrs_3 = ['a_29', 'a_21', 'a_50', 'a_72', 'a_58', 'a_5', 'a_73', 'a_44', 'a_81', 'a_77', 'a_38', 'a_18', 'a_27', 'a_41', 'a_40', 'a_36', 'a_80', 'a_32']






