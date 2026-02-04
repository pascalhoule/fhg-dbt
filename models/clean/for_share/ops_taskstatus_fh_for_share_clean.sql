{{  config(alias='ops_taskstatus_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('taskstatus_vc_clean_insurance') }}