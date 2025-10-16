{{  config(alias='itm_brokeradvanced', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('itm_brokeradvanced_for_share_clean') }}