-------------------------------------------------patients table-----------------------------------------

CREATE TABLE patients (
    subject_id integer NOT NULL,
    gender character varying(5) NOT NULL,
    dob timestamp(0) without time zone NOT NULL,
    dod timestamp(0) without time zone,
    dod_hosp timestamp(0) without time zone,
    dod_ssn timestamp(0) without time zone,
    expire_flag integer NOT NULL,

    primary key(subject_id)
);

-- \copy (select subject_id,gender,dob,dod,dod_hosp,dod_ssn,expire_flag from patients) to '/home/perm/chenjie/mimic_cleaned/patients.csv' csv header;

-- \copy patients from '/home/perm/chenjie/mimic_cleaned/patients.csv' csv header;

----------------------------------------------------------admissions table-------------------------------------------------------------

-- CREATE MATERIALIZED VIEW admissions_new AS 
-- (
-- 	select
-- 	hadm_id,
-- 	admittime,
-- 	dischtime,
-- 	deathtime,
-- 	admission_type,
-- 	admission_location,
-- 	discharge_location,
-- 	insurance,
-- 	marital_status,
-- 	edregtime,
-- 	edouttime,
-- 	diagnosis,
-- 	hospital_expire_flag,
-- 	has_chartevents_data,
-- 	CASE
-- 	    WHEN deathtime > admittime
-- 	        THEN ROUND((cast(deathtime as date) - cast(admittime as date)), 2)
-- 	    WHEN discharge_location = 'DEAD/EXPIRED'
-- 	        THEN ROUND((cast(dischtime as date) - cast(admittime as date)), 2)
-- 	    WHEN hospital_expire_flag = 0
-- 	        THEN ROUND((cast(dischtime as date) - cast(admittime as date)), 2)
-- 	    ELSE -1
-- 	    END AS hospital_stay_length
-- 	from admissions
-- );

CREATE TABLE admissions(
hadm_id integer NOT NULL,
admittime timestamp(0) without time zone NOT NULL,
dischtime timestamp(0) without time zone NOT NULL,
deathtime timestamp(0) without time zone,
admission_type character varying(50) NOT NULL,
admission_location character varying(50) NOT NULL,
discharge_location character varying(50) NOT NULL,
insurance character varying(255) NOT NULL,
marital_status character varying(50),
edregtime timestamp(0) without time zone,
edouttime timestamp(0) without time zone,
diagnosis character varying(255),
hospital_expire_flag smallint,
has_chartevents_data smallint NOT NULL,
hospital_stay_length float,

primary key(hadm_id)
);


-- \copy (select hadm_id,admittime,dischtime,deathtime,admission_type,admission_location,discharge_location,insurance,marital_status,edregtime,edouttime,diagnosis,hospital_expire_flag,has_chartevents_data,hospital_stay_length from admissions_new) to '/home/perm/chenjie/mimic_cleaned/admissions.csv' csv header;

-- \copy admissions from '/home/perm/chenjie/mimic_cleaned/admissions.csv' csv header;


-------------------------------------------------------patients_admit_info-------------------------------------------------

CREATE TABLE patients_admit_info(
	subject_id integer NOT NULL,
	hadm_id integer NOT NULL,
	age numeric(5,2),
	language character varying(10),
	religion character varying(50),
	ethnicity character varying(200) NOT NULL,

	primary key(subject_id, hadm_id),

	foreign key(subject_id) references patients(subject_id),
	foreign key(hadm_id) references admissions(hadm_id)
	);


-- get age 

-- CREATE MATERIALIZED VIEW patients_admit_info AS
-- WITH admission_time AS
-- (
--   SELECT
--       p.subject_id, a.hadm_id, p.dob, p.gender, a.admittime
--       , ROUND( (cast(admittime as date) - cast(dob as date)) / 365.242,2) 
--           AS admit_age
--   FROM patients p
--   INNER JOIN admissions a
--   ON p.subject_id = a.subject_id
-- )
-- SELECT a.subject_id, a.hadm_id, 
-- CASE 
-- WHEN at.admit_age > 100 THEN 90
-- ELSE at.admit_age 
-- END AS age,
-- a.language, a.religion, a.ethnicity 
-- from admissions a, admission_time at
-- where a.subject_id = at.subject_id AND a.hadm_id = at.hadm_id

-- \copy (select * from patients_admit_info) to '/home/perm/chenjie/mimic_cleaned/patients_admt_info.csv' csv header;

-- \copy patients_admit_info from '/home/perm/chenjie/mimic_cleaned/patients_admt_info.csv' csv header;


----------------------------------------------------------table diagnosis--------------------------------------------------------

-- CREATE MATERIALIZED VIEW diagnoses_new as
-- (WITH temp_icd9_code as (select subject_id,
-- hadm_id,seq_num,icd9_code, SUBSTRING(icd9_code, 1, 3) as code_first3 from diagnoses_icd)
-- select 
-- subject_id,
-- hadm_id,seq_num,
-- icd9_code,
-- 	CASE 
--     WHEN SUBSTRING(icd9_code, 1, 1) in ('E', 'V', 'M')
--         THEN SUBSTRING(icd9_code, 1, 1)
--     WHEN CAST(code_first3 as INT) <= 139
--         THEN '1'
--     WHEN CAST(code_first3 as INT) <= 239
--         THEN '2'
-- 	WHEN CAST(code_first3 as INT) <= 279
--         THEN '3'
--     WHEN CAST(code_first3 as INT) <= 289
--         THEN '4'        
--     WHEN CAST(code_first3 as INT) <= 319
--         THEN '5'
--     WHEN CAST(code_first3 as INT) <= 389
--         THEN '6'
-- 	WHEN CAST(code_first3 as INT) <= 459
--         THEN '7'
--     WHEN CAST(code_first3 as INT) <= 519
--         THEN '8'        
--     WHEN CAST(code_first3 as INT) <= 579
--         THEN '9'
--     WHEN CAST(code_first3 as INT) <= 629
--         THEN '10'
-- 	WHEN CAST(code_first3 as INT) <= 679
--         THEN '11'
--     WHEN CAST(code_first3 as INT) <= 709
--         THEN '12'        
--     WHEN CAST(code_first3 as INT) <= 739
--         THEN '13'
--     WHEN CAST(code_first3 as INT) <= 759
--         THEN '14'
-- 	WHEN CAST(code_first3 as INT) <= 779
--         THEN '15'
--     WHEN CAST(code_first3 as INT) <= 799
--         THEN '16'        
--     WHEN CAST(code_first3 as INT) <= 999
--         THEN '17'        
--     ELSE '-1'
--     END AS chapter
--  from temp_icd9_code)



CREATE TABLE diagnoses(
	subject_id integer NOT NULL,
	hadm_id integer NOT NULL,
	seq_num integer,
	icd9_code character varying(10),
	chapter character varying(10),

	primary key (subject_id, hadm_id, seq_num),

	foreign key (subject_id) references patients(subject_id),
	foreign key (hadm_id) references admissions(hadm_id)
);

\copy (select subject_id, hadm_id, seq_num, icd9_code, chapter from diagnoses_new) to '/home/perm/chenjie/mimic_cleaned/diagnoses.csv' csv header;
-- \copy diagnoses from '/home/perm/chenjie/mimic_cleaned/diagnoses.csv' csv header;


-------------------------------------------------procedures table ------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW procedures_new as
-- (WITH temp_icd9_code as (select subject_id,
-- hadm_id,seq_num,icd9_code, SUBSTRING(icd9_code, 1, 2) as code_first2 from procedures_icd)
-- select 
-- subject_id,
-- hadm_id,
-- seq_num,
-- icd9_code,
-- 	CASE 
--     WHEN CAST(code_first2 as INT) = 0
--         THEN '0'    
-- 	WHEN CAST(code_first2 as INT) <= 5
--         THEN '1'
--     WHEN CAST(code_first2 as INT) <= 7
--         THEN '2'
-- 	WHEN CAST(code_first2 as INT) <= 16
--         THEN '3'
--     WHEN CAST(code_first2 as INT) <= 17
--         THEN '3A'        
--     WHEN CAST(code_first2 as INT) <= 20
--         THEN '4'        
--     WHEN CAST(code_first2 as INT) <= 29
--         THEN '5'
--     WHEN CAST(code_first2 as INT) <= 34
--         THEN '6'
-- 	WHEN CAST(code_first2 as INT) <= 39
--         THEN '7'
--     WHEN CAST(code_first2 as INT) <= 41
--         THEN '8'        
--     WHEN CAST(code_first2 as INT) <= 54
--         THEN '9'
--     WHEN CAST(code_first2 as INT) <= 59
--         THEN '10'
-- 	WHEN CAST(code_first2 as INT) <= 64
--         THEN '11'
--     WHEN CAST(code_first2 as INT) <= 71
--         THEN '12'        
--     WHEN CAST(code_first2 as INT) <= 75
--         THEN '13'
--     WHEN CAST(code_first2 as INT) <= 84
--         THEN '14'
-- 	WHEN CAST(code_first2 as INT) <= 86
--         THEN '15'
--     WHEN CAST(code_first2 as INT) <= 99
--         THEN '16'            
--     ELSE '-1'
--     END AS chapter
--  from temp_icd9_code)


CREATE TABLE procedures (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    seq_num integer NOT NULL,
    icd9_code character varying(10) NOT NULL,
    chapter character varying(10),

    primary key (subject_id, hadm_id, seq_num),

    foreign key (subject_id) references patients(subject_id),
    foreign key (hadm_id) references admissions(hadm_id)
);

-- \copy (select subject_id, hadm_id, seq_num, icd9_code, chapter from procedures_new) to '/home/perm/chenjie/mimic_cleaned/procedures.csv' csv header;
-- \copy procedures from '/home/perm/chenjie/mimic_cleaned/procedures.csv' csv header;


------------------------------ICU_STAYS-------------------------------------------------------

-- CREATE MATERIALIZED VIEW icustays_grouped AS
--  SELECT icustays.row_id,
--     icustays.subject_id,
--     icustays.hadm_id,
--     icustays.icustay_id,
--     icustays.dbsource,
--     icustays.first_careunit,
--     icustays.last_careunit,
--     icustays.first_wardid,
--     icustays.last_wardid,
--     icustays.intime,
--     icustays.outtime,
--     icustays.los,
--         CASE
--             WHEN (icustays.los <= (1)::double precision) THEN '0-1'::text
--             WHEN (icustays.los <= (2)::double precision) THEN '1-2'::text
--             WHEN (icustays.los <= (4)::double precision) THEN '2-4'::text
--             WHEN (icustays.los <= (8)::double precision) THEN '4-8'::text
--             ELSE 'x>8'::text
--         END AS los_group
   -- FROM mimiciii.icustays

CREATE TABLE icustays(
	subject_id integer NOT NULL,
	hadm_id integer NOT NULL,
	icustay_id integer NOT NULL,
	dbsource character varying(20) NOT NULL,
	first_careunit character varying(20) NOT NULL,
	last_careunit character varying(20) NOT NULL,
	first_wardid smallint NOT NULL,
	last_wardid smallint NOT NULL,
	intime timestamp(0) without time zone NOT NULL,
	outtime timestamp(0) without time zone,
	los double precision,
	los_group character varying(20),

	primary key (subject_id,hadm_id,icustay_id),

	foreign key (subject_id) references patients(subject_id),
	foreign key (hadm_id) references admissions(hadm_id)
);

-- \copy (select subject_id,hadm_id,icustay_id,dbsource,first_careunit,last_careunit,first_wardid,last_wardid,intime,outtime,los,los_group from icustays_grouped) to '/home/perm/chenjie/mimic_cleaned/icustays.csv' csv header;

-- \copy icustays from '/home/perm/chenjie/mimic_cleaned/icustays.csv' csv header;

