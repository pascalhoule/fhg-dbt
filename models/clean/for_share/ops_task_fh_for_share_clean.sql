{{  config(alias='ops_task_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('task_fh_ops_taskreport') }}