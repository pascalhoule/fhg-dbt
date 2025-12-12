{{  config(alias='policytasks_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('policytasks_vc_clean_insurance')  }}