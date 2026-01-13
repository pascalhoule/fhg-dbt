{{  config(alias='aga', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('aga_integrate_insurance')  }}