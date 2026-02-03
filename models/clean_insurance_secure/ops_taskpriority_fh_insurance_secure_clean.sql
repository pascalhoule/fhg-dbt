{{  config(alias='ops_taskpriority_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ops_taskpriority_fh_for_share_clean') }}