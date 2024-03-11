{{  config(alias='brokercontract_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('brokercontract_vc_integrate_insurance')  }}