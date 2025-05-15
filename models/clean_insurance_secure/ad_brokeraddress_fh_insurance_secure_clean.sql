{{  config(alias='ad_brokeraddress_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_brokeraddress_fh_for_share_clean') }}