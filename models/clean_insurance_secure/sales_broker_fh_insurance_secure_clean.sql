{{  config(alias='sales_broker_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('sales_broker_fh_for_share_clean') }}