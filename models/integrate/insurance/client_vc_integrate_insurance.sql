{{  config(alias='client_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
from {{ ref ('client_vc_normalize_insurance')  }}