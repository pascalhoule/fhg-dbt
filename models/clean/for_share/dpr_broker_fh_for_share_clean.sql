{{  config(alias='dpr_broker_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('broker_report_daily_apps') }}