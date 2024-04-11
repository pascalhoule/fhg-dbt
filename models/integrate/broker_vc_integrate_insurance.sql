{{  config(alias='broker_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
from {{ ref ('broker_vc_normalize_insurance')  }}