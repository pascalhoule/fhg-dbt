{{  config(alias='commissionrun', database='finance', schema='insurance', materialization = "view")  }} 

SELECT * 
  


from {{ ref ('commissionrun_clean_insurance')  }}