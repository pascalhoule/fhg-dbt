{{  config(alias='ic', database='analyze', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('ic_integrate_insurance') }}