{{  config(alias='commission_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('commission_vc_clean_insurance')  }}