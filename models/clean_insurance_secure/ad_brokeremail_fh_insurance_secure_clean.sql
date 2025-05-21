{{  config(alias='ad_brokeremail_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_brokeremail_fh_for_share_clean') }}