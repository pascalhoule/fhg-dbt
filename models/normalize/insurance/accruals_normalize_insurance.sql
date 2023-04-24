 {{  config(alias='accruals', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('accruals_clean_insurance')  }}