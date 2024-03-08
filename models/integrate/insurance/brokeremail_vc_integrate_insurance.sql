{{  config(alias='brokeremail_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
from {{ ref ('brokeremail_vc_normalize_insurance')  }}