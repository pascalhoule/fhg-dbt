{{  config(alias='brokercontracttype_vc', database='integrate', schema='insurance')  }} 
 

SELECT * 
  
FROM {{ ref ('brokercontracttype_vc_normalize_insurance')  }}