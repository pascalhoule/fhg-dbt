{{  config(alias='brokeraddress_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('brokeraddress_vc_integrate_insurance')  }}