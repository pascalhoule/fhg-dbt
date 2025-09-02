{{  config(alias='dpr_hierarchy_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('hierarchy_fh_report_daily_apps') }}