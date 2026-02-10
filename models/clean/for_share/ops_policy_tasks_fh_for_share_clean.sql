{{  config(alias='ops_policy_tasks_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('policy_tasks_ops_taskreport') }}