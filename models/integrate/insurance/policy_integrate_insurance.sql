 {{  config(alias='policy', database='integrate', schema='insurance')  }} 
 


SELECT * 
  


from {{ ref ('policy_normalize_insurance')  }}