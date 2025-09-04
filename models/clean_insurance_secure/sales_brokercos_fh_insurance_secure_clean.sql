{{  config(alias='sales_brokercos_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('sales_brokercos_fh_for_share_clean') }}