{{  config(alias='brokercontractstatus_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
FROM {{ ref ('brokercontractstatus_vc_normalize_insurance')  }}