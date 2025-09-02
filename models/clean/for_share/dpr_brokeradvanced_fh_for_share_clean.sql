{{  config(alias='dpr_brokeradvanced_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokeradvanced_report_daily_apps') }}