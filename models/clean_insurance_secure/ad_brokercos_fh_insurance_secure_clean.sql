{{  config(alias='ad_brokercos_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_brokercos_fh_for_share_clean') }}