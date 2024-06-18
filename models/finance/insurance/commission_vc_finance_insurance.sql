{{  config(alias='commission_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('commission_vc_clean_insurance')  }}