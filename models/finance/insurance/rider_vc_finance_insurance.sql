{{  config(alias='rider_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('rider_vc_clean_insurance')  }}