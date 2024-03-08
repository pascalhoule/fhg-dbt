{{  config(alias='brokercontractstatus_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('brokercontractstatus_vc_integrate_insurance')  }}