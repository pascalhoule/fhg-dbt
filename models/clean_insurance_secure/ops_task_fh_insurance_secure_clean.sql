{{  config(alias='ops_task_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ops_task_fh_for_share_clean') }}