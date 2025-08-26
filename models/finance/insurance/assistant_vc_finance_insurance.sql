{{  config(alias='assistant_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('assistant_vc_clean_insurance')  }}