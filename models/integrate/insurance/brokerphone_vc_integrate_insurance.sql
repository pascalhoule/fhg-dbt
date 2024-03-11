{{  config(alias='brokerphone_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
from {{ ref ('brokerphone_vc_normalize_insurance')  }}