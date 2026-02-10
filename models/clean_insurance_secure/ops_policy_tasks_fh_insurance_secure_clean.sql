{{  config(alias='ops_policy_tasks_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ops_policy_tasks_fh_for_share_clean') }}