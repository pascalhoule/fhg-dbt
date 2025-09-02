{{  config(alias='dpr_brokeradvanced_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('dpr_brokeradvanced_fh_for_share_clean') }}