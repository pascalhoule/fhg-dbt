{{  config(alias='ad_brokercontractstatus_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokercontractstatus_fh_report_advisor_details') }}