 {{  config(alias='importednbpolicy', database='integrate', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('importednbpolicy_normalize_insurance')  }}