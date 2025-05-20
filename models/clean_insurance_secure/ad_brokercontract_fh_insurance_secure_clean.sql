{{  config(alias='ad_brokercontract_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_brokercontract_fh_for_share_clean') }}