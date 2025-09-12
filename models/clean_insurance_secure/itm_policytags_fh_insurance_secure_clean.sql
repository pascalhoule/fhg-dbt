{{  config(alias='itm_policytags_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('itm_policytags_fh_for_share_clean') }}