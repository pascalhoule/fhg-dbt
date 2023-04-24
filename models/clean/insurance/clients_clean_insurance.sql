 {{ config(alias='clients', database='clean', schema='insurance') }}

SELECT

 c.code
,c.clientid
,c.first_name
,c.last_name
,c.jointaccountstatus
,c.address_attn_line
,c.address_attn_prefix 
,c.cell_phone
,c.home_phone
,c.work_phone
,c.extention_phone
,c.other_phone
,c.dob
,c.sex
,c.smoker
,c.email
,c.email2
,c.country_code
,c.city
,c.street
,c.street2
,c.street3
,c.zipcode
,c.state_code as state_code_raw
,case when rtrim(c.state_code) = '' then null else rtrim(c.state_code) end as state_code
,c.client_language
,c.agentcode
,c.datalake_timestamp as _infx_loaded_ts_utc 
,c.datalake_timestamp as _infx_active_from_ts_utc 
,case when c.datalake_end_ts = '3000-01-01 00:00:00.000' then '9999-12-31 23:59:59' else c.datalake_end_ts end as _infx_active_to_ts_utc 
,case when c.datalake_end_ts = '3000-01-01 00:00:00.000' then true else false end as _infx_is_active

FROM {{ source('insurance','clients') }} c
-- inner join {{ source('insurance','state') }} s on s.code = rtrim(c.state_code) -- to be deleted (discuss with FHG)
where c.status < 100
