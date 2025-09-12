{{  config(alias='itm_broker', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('itm_broker_for_share_clean') }}