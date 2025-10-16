{{  config(alias='policytasks_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('policytasks_vc_clean_insurance')  }}