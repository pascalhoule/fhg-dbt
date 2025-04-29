{{  config(alias='ins_brokercontract_vc', 
database='clean', 
schema='insurance_secure')  }} 

SELECT *
FROM {{ ref('brokercontract_vc_clean_insurance') }}