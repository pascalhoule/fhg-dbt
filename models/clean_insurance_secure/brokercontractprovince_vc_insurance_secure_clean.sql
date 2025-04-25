{{  config(alias='ins_brokercontractprovince_vc', 
database='clean', 
schema='insurance_secure')  }} 

SELECT *
FROM {{ ref('brokercontractprovince_vc_clean_insurance') }}