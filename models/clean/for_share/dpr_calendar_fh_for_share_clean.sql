{{  config(alias='dpr_calendar_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('calendar_report_daily_apps') }}