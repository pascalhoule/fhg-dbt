{{  config(alias='ops_tasktype_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ops_tasktype_fh_for_share_clean') }}