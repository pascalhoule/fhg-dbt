{{  config(alias='brokercontracttype_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('brokercontracttype_vc_integrate_insurance')  }}