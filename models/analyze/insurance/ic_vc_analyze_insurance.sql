{{  config(alias='ic_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('ic_vc_integrate_insurance')  }}