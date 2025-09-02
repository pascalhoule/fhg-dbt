{{  config(alias='dpr_cos_serv_agt_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('cos_serv_agt_report_daily_apps') }}