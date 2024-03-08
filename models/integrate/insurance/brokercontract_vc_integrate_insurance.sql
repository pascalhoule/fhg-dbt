{{  config(alias='brokercontract_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
FROM {{ ref ('brokercontract_vc_normalize_insurance')  }}