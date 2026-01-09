{{  config(alias='assistant_vc', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('assistant_vc_clean_insurance')  }}