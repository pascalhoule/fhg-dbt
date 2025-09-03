{{  config(alias='dpr_policy_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('policy_fh_report_daily_apps') }}