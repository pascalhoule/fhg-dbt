{{  config(alias='ad_brokeraddress_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('brokeraddress_fh_advisor_details') }}