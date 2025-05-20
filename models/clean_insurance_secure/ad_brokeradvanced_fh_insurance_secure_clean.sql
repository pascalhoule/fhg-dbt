{{  config(alias='ad_brokeradvanced_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_brokeradvanced_fh_for_share_clean') }}