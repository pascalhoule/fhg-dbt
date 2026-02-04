{{  config(alias='ops_task_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('tasks_vc_clean_insurance') }}