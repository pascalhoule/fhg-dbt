{{  config(alias='brokeraddress_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
from {{ ref ('brokeraddress_vc_normalize_insurance')  }}