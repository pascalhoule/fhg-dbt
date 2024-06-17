{{  config(alias='accruals', database='finance', schema='insurance', materialization = "view")  }} 


SELECT * 
  


from {{ ref ('accruals_clean_insurance')  }}