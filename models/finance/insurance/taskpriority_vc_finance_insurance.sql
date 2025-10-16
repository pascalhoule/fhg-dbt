{{  config(alias='taskpriority_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('taskpriority_vc_clean_insurance')  }}