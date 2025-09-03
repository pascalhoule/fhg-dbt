{{  config(alias='dpr_brokertags_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('dpr_brokertags_fh_for_share_clean') }}