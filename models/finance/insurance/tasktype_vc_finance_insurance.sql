{{  config(alias='tasktype_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('tasktype_vc_clean_insurance')  }}