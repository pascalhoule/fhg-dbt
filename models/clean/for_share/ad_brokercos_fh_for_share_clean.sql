{{  config(alias='ad_brokercos_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokercos_fh_report_advisor_details') }}