{{  config(alias='policyrider_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('policyrider_vc_clean_insurance')  }}