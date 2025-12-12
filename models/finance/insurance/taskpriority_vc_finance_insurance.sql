{{  config(alias='taskpriority_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('taskpriority_vc_clean_insurance')  }}