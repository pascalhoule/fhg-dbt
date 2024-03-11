{{  config(alias='brokercontractprovince_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
from {{ ref ('brokercontractprovince_vc_normalize_insurance')  }}