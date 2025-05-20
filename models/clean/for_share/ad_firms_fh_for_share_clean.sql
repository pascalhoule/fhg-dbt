{{  config(alias='ad_firms_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('firms_fh_report_advisor_details') }}