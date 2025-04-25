{{  config(alias='ins_broker_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT *
FROM {{ ref('broker_fh_integrate_insurance') }}