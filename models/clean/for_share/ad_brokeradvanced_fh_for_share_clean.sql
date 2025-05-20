{{  config(alias='ad_brokeradvanced_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokeradvanced_fh_report_advisor_details') }}