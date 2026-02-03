{{  config(alias='ops_taskstatus_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ops_taskstatus_fh_for_share_clean') }}