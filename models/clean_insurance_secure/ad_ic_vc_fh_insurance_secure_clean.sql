{{  config(alias='ad_ic_vc_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_ic_vc_fh_for_share_clean') }}