{{  config(alias='brokeradvanced_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('brokeradvanced_vc_integrate_insurance')  }}