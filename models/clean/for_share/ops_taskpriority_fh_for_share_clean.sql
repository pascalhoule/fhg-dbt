{{  config(alias='ops_taskpriority_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('taskpriority_vc_clean_insurance') }}