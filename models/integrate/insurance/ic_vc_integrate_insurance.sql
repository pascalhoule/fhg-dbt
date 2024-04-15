{{  config(alias='ic_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
from {{ ref ('ic_vc_normalize_insurance')  }}