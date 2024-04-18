 {{  config(alias='policy_fh', database='integrate', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('policy_fh_normalize_insurance')  }}