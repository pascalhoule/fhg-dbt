 {{  config(alias='fundaccount_consultant', database='clean', schema='investment')  }} 


SELECT fa.* 
 ,fa.datalake_timestamp as _infx_loaded_ts_utc  
 ,fa.datalake_timestamp as _infx_active_from_ts_utc 
 ,case when fa.datalake_end_ts = '3000-01-01 00:00:00.000' then '9999-12-31 23:59:59' else fa.datalake_end_ts end as _infx_active_to_ts_utc  
 ,case when fa.datalake_end_ts = '3000-01-01 00:00:00.000' then true else false end as _infx_is_active 



from {{ source ('investment', 'fundaccount')  }} fa
inner join {{ source ('investment', 'registration')  }} reg on reg.code = fa.registration_code
inner join {{ source ('investment', 'clients')  }} cl on cl.code = reg.kyc_code
where fa.account_status = 0 -- active