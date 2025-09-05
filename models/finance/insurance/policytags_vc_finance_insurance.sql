{{  config(alias='policytags_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('policytags_vc_clean_insurance')  }}