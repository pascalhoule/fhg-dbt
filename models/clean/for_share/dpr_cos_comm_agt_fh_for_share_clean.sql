{{  config(alias='dpr_cos_comm_agt_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('cos_comm_agt_report_daily_apps') }}