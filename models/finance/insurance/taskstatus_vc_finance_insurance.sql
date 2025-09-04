{{  config(alias='taskstatus_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('taskstatus_vc_clean_insurance')  }}