 {{  config(alias='importednbpolicy', database='normalize', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('importednbpolicy_clean_insurance')  }}