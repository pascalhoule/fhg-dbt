{{  config(alias='policyclientlinking', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('policyclientlinking_integrate_insurance')  }}