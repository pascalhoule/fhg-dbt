{{  config(alias='brokerphone_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('brokerphone_vc_integrate_insurance')  }}