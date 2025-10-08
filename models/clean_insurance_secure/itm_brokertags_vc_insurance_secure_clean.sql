{{  config(alias='itm_brokertags_vc', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('itm_brokertags_vc_for_share_clean') }}