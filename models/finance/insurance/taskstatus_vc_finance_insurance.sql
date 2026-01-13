{{  config(alias='taskstatus_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('taskstatus_vc_clean_insurance')  }}