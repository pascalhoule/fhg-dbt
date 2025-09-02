{{  config(alias='ad_brokerphone_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_brokerphone_fh_for_share_clean') }}