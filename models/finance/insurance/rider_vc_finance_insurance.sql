{{  config(alias='rider_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('rider_vc_clean_insurance')  }}