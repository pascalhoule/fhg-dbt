 {{  config(alias='accruals', database='analyze', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('accruals_integrate_insurance')  }}