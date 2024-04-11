{{  config(alias='broker_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('broker_vc_integrate_insurance')  }}