 {{  config(alias='accruals', database='integrate', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('accruals_normalize_insurance')  }}