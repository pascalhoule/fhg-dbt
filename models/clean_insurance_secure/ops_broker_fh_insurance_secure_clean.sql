{{  config(alias='ops_broker_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ops_broker_fh_for_share_clean') }}