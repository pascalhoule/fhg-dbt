 {{  config(alias='policy_vc', database='analyze', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('policy_vc_integrate_insurance')  }}