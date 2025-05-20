{{  config(alias='ad_brokercontract_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokercontract_fh_report_advisor_details') }}