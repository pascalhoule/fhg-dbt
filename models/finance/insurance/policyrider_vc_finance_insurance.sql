{{  config(alias='policyrider_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  
from {{ ref ('policyrider_vc_clean_insurance')  }}