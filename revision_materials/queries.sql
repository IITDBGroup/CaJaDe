WITH gt AS
(
select p_desc, is_user from feb11_med_vs_private_sample_rate_eval.global_results 
where  exp_desc = 'death rate: medicare vs private__mimic_rev__0.05__2__0.0__y__f__o__0.05__1000' 
)
select count(*) from feb11_med_vs_private_sample_rate_eval.global_results s, gt 
where  exp_desc = 'death rate: medicare vs private__mimic_rev__0.05__2__0.0__y__f__s__0.05__1000' 
and s.p_desc = gt.p_desc and s.is_user = gt.is_user


WITH gt AS
(
select p_desc, is_user from feb11_med_vs_private_sample_rate_eval.global_results 
where  exp_desc = 'death rate: medicare vs private__mimic_rev__0.05__2__0.0__y__f__o__0.05__1000' 
)
select count(*) from feb11_med_vs_private_sample.global_results s, gt 
where  exp_desc = 'death rate: medicare vs private__mimic_rev__0.05__2__0.0__y__f__s__0.4__1000' 
and s.p_desc = gt.p_desc and s.is_user = gt.is_user

# LCAs

select t.id,t.time,t.sample_size,t.apt_size,t.num_result_p,
t.num_attrs,t.result_schema,t.exp_desc, count(*) as num_match
from test_jg_288.lca_exp_stats t, test_jg_288.patterns as tp
where tp.exp_desc = t.exp_desc and tp.pattern in
(
	SELECT pattern from gt_jg288
)
group by t.id,t.time,t.sample_size,t.apt_size,t.num_result_p,
t.num_attrs,t.result_schema,t.exp_desc


select id, result_schema, time, apt_size, num_result_p, num_attrs, sample_size, exp_desc
from test_jg_288.patterns   




    select t.id,t.time,t.sample_size,t.apt_size,t.num_result_p,
    t.num_attrs,t.result_schema,t.exp_desc, count(*) as num_match
    from test_jg_31.lca_exp_stats t, test_jg_31.patterns as tp
    where tp.exp_desc = t.exp_desc and tp.pattern in
    (
    select pattern from test_jg_31.patterns where exp_desc = (
    select exp_desc  
    from test_jg_31.lca_exp_stats 
    order by sample_size::numeric 
    desc limit 1
    )
    )
    group by t.id,t.time,t.sample_size,t.apt_size,t.num_result_p,
    t.num_attrs,t.result_schema,t.exp_desc





-- jg_24 MIMIC LCA

SELECT COUNT(distinct a_2) as a_2,COUNT(distinct a_3) as a_3,COUNT(distinct a_4) as a_4,
COUNT(distinct a_5) as a_5,COUNT(distinct a_6) as a_6,COUNT(distinct a_7) as a_7,COUNT(distinct a_9) as a_9,
COUNT(distinct a_10) as a_10,COUNT(distinct a_11) as a_11,COUNT(distinct a_12) as a_12,COUNT(distinct a_16) as a_16,
COUNT(distinct a_17) as a_17,COUNT(distinct a_18) as a_18,COUNT(distinct a_21) as a_21,COUNT(distinct a_22) as a_22,
COUNT(distinct a_23) as a_23,COUNT(distinct a_24) as a_24,COUNT(distinct a_25) as a_25
FROM jg_24;

a_18=WHITE,
*
a_26=0
a_18=WHITE,a_26=0
a_5=EMERGENCY,
a_18=WHITE,a_21=M,
a_5=EMERGENCY,a_18=WHITE,
a_21=M,
a_21=F,
a_18=WHITE,a_21=F,


-- jg_1 MIMIC LCA

SELECT COUNT(DISTINCT a_2) AS a_2,COUNT(DISTINCT a_3) AS a_3,COUNT(DISTINCT a_4) AS a_4,COUNT(DISTINCT a_5) AS a_5,COUNT(DISTINCT a_6) AS a_6,
COUNT(DISTINCT a_7) AS a_7,COUNT(DISTINCT a_9) AS a_9,COUNT(DISTINCT a_10) AS a_10,COUNT(DISTINCT a_11) AS a_11,COUNT(DISTINCT a_12) AS a_12
FROM jg_1;


SELECT * FROM 
(
(
SELECT id, result_schema, time, sample_size, 
round(sample_size::numeric/apt_size::numeric,2) as sample_rate,
num_match,
apt_size,
num_attrs
from lca_jg_288.result
)
union 
(
SELECT id, result_schema, time, sample_size, 
round(sample_size::numeric/apt_size::numeric,2) as sample_rate,
num_match,
apt_size,
num_attrs
from lca_jg_31.result
) 
) r order by result_schema, id


SELECT * FROM 
(
(
SELECT id, result_schema, time, sample_size, 
round(sample_size::numeric/apt_size::numeric,2) as sample_rate,
num_match,
apt_size,
num_attrs
from lca_jg_24.result
)
union 
(
SELECT id, result_schema, time, sample_size, 
round(sample_size::numeric/apt_size::numeric,2) as sample_rate,
num_match,
apt_size,
num_attrs
from lca_jg_9.result
)
union 
(
SELECT id, result_schema, time, sample_size, 
round(sample_size::numeric/apt_size::numeric,2) as sample_rate,
num_match,
apt_size,
num_attrs
from lca_jg_1.result
)  
) r order by result_schema, id



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5

-- NDCG measure

