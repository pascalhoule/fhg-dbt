{{  config(alias='tasktype_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('tasktype_vc_clean_insurance')  }}