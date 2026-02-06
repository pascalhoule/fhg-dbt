{{  config(alias='ops_tasks1_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('tasks_vc_clean_insurance') }}