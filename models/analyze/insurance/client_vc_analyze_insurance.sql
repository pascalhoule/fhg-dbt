{{  config(alias='client_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('client_vc_integrate_insurance')  }}