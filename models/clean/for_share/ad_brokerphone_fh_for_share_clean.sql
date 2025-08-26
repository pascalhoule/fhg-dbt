{{  config(alias='ad_brokerphone_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokerphone_fh_report_advisor_details') }}