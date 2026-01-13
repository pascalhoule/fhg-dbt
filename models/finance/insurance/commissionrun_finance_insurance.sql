{{  config(alias='commissionrun', database='finance', schema='insurance', materialized = "view")  }} 

SELECT * 
  


from {{ ref ('commissionrun_clean_insurance')  }}