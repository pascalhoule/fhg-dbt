{{  config(alias='ad_firms_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_firms_fh_for_share_clean') }}