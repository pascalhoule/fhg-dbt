{{  config(alias='ad_broker_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('broker_fh_report_advisor_details') }}