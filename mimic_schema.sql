--
-- PostgreSQL database dump
--

-- Dumped from database version 10.15 (Ubuntu 10.15-1.pgdg18.04+1)
-- Dumped by pg_dump version 10.15 (Ubuntu 10.15-1.pgdg18.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: _get_explain_json(text); Type: FUNCTION; Schema: public; Owner: lchenjie
--

CREATE FUNCTION public._get_explain_json(qry text, OUT r jsonb) RETURNS SETOF jsonb
    LANGUAGE plpgsql
    AS $$declare explcmd text; begin explcmd := ('EXPLAIN (FORMAT JSON) ' || qry); for r in execute explcmd loop return next; end loop; return; end; $$;


ALTER FUNCTION public._get_explain_json(qry text, OUT r jsonb) OWNER TO lchenjie;

--
-- Name: cost_estimation(text); Type: FUNCTION; Schema: public; Owner: lchenjie
--

CREATE FUNCTION public.cost_estimation(qry text, OUT r jsonb) RETURNS SETOF jsonb
    LANGUAGE plpgsql
    AS $$ 
        declare  
        explcmd text;  
        begin  
        explcmd := ('EXPLAIN (FORMAT JSON) ' || qry);  
        for r in execute explcmd loop  
        return next;  
        end loop;  
        return;  
        end; $$;


ALTER FUNCTION public.cost_estimation(qry text, OUT r jsonb) OWNER TO lchenjie;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: admissions; Type: TABLE; Schema: public; Owner: lchenjie
--

CREATE TABLE public.admissions (
    hadm_id integer NOT NULL,
    admittime text NOT NULL,
    dischtime text NOT NULL,
    deathtime text,
    admission_type character varying(50) NOT NULL,
    admission_location character varying(50) NOT NULL,
    discharge_location character varying(50) NOT NULL,
    insurance character varying(255) NOT NULL,
    marital_status character varying(50),
    edregtime text,
    edouttime text,
    diagnosis character varying(255),
    hospital_expire_flag smallint,
    hospital_stay_length double precision
);


ALTER TABLE public.admissions OWNER TO lchenjie;

--
-- Name: diagnoses; Type: TABLE; Schema: public; Owner: lchenjie
--

CREATE TABLE public.diagnoses (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    seq_num integer NOT NULL,
    icd9_code character varying(10),
    chapter character varying(10)
);


ALTER TABLE public.diagnoses OWNER TO lchenjie;

--
-- Name: icustays; Type: TABLE; Schema: public; Owner: lchenjie
--

CREATE TABLE public.icustays (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    icustay_id integer NOT NULL,
    dbsource character varying(20) NOT NULL,
    first_careunit character varying(20) NOT NULL,
    last_careunit character varying(20) NOT NULL,
    first_wardid text NOT NULL,
    last_wardid text NOT NULL,
    intime text NOT NULL,
    outtime text,
    los double precision,
    los_group character varying(20)
);


ALTER TABLE public.icustays OWNER TO lchenjie;

--
-- Name: patients; Type: TABLE; Schema: public; Owner: lchenjie
--

CREATE TABLE public.patients (
    subject_id integer NOT NULL,
    gender character varying(5) NOT NULL,
    dob text NOT NULL,
    dod text,
    dod_hosp text,
    dod_ssn text
);


ALTER TABLE public.patients OWNER TO lchenjie;

--
-- Name: patients_admit_info; Type: TABLE; Schema: public; Owner: lchenjie
--

CREATE TABLE public.patients_admit_info (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    age numeric(5,2),
    language character varying(10),
    religion character varying(50),
    ethnicity character varying(200) NOT NULL
);


ALTER TABLE public.patients_admit_info OWNER TO lchenjie;

--
-- Name: procedures; Type: TABLE; Schema: public; Owner: lchenjie
--

CREATE TABLE public.procedures (
    subject_id integer NOT NULL,
    hadm_id integer NOT NULL,
    seq_num integer NOT NULL,
    icd9_code character varying(10) NOT NULL,
    chapter character varying(10)
);


ALTER TABLE public.procedures OWNER TO lchenjie;

--
-- Name: admissions admissions_pkey; Type: CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.admissions
    ADD CONSTRAINT admissions_pkey PRIMARY KEY (hadm_id);


--
-- Name: diagnoses diagnoses_pkey; Type: CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.diagnoses
    ADD CONSTRAINT diagnoses_pkey PRIMARY KEY (subject_id, hadm_id, seq_num);


--
-- Name: icustays icustays_pkey; Type: CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.icustays
    ADD CONSTRAINT icustays_pkey PRIMARY KEY (subject_id, hadm_id, icustay_id);


--
-- Name: patients_admit_info patients_admit_info_pkey; Type: CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.patients_admit_info
    ADD CONSTRAINT patients_admit_info_pkey PRIMARY KEY (subject_id, hadm_id);


--
-- Name: patients patients_pkey; Type: CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.patients
    ADD CONSTRAINT patients_pkey PRIMARY KEY (subject_id);


--
-- Name: procedures procedures_pkey; Type: CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.procedures
    ADD CONSTRAINT procedures_pkey PRIMARY KEY (subject_id, hadm_id, seq_num);


--
-- Name: diagnoses diagnoses_hadm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.diagnoses
    ADD CONSTRAINT diagnoses_hadm_id_fkey FOREIGN KEY (hadm_id) REFERENCES public.admissions(hadm_id);


--
-- Name: diagnoses diagnoses_subject_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.diagnoses
    ADD CONSTRAINT diagnoses_subject_id_fkey FOREIGN KEY (subject_id) REFERENCES public.patients(subject_id);


--
-- Name: icustays icustays_hadm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.icustays
    ADD CONSTRAINT icustays_hadm_id_fkey FOREIGN KEY (hadm_id) REFERENCES public.admissions(hadm_id);


--
-- Name: icustays icustays_subject_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.icustays
    ADD CONSTRAINT icustays_subject_id_fkey FOREIGN KEY (subject_id) REFERENCES public.patients(subject_id);


--
-- Name: patients_admit_info patients_admit_info_hadm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.patients_admit_info
    ADD CONSTRAINT patients_admit_info_hadm_id_fkey FOREIGN KEY (hadm_id) REFERENCES public.admissions(hadm_id);


--
-- Name: patients_admit_info patients_admit_info_subject_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.patients_admit_info
    ADD CONSTRAINT patients_admit_info_subject_id_fkey FOREIGN KEY (subject_id) REFERENCES public.patients(subject_id);


--
-- Name: procedures procedures_hadm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.procedures
    ADD CONSTRAINT procedures_hadm_id_fkey FOREIGN KEY (hadm_id) REFERENCES public.admissions(hadm_id);


--
-- Name: procedures procedures_subject_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: lchenjie
--

ALTER TABLE ONLY public.procedures
    ADD CONSTRAINT procedures_subject_id_fkey FOREIGN KEY (subject_id) REFERENCES public.patients(subject_id);


--
-- PostgreSQL database dump complete
--

