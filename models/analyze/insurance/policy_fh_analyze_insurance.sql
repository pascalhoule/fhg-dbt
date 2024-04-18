 {{  config(alias='policy_fh', database='analyze', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('policy_fh_integrate_insurance')  }}