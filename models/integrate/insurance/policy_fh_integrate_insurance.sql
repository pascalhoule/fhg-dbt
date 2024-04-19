 {{  config(alias='policy_fh', database='integrate', schema='insurance', tags=["policy_fh"])  }} 


SELECT * 
  


from {{ ref ('policy_fh_normalize_insurance')  }}