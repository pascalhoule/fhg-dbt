{{  config(alias='ops_tasktype_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('tasktype_vc_clean_insurance') }}