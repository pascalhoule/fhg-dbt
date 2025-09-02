{{  config(alias='dpr_broker_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('dpr_broker_fh_for_share_clean') }}