{{  config(alias='accruals', database='finance', schema='insurance', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('accruals_clean_insurance')  }}