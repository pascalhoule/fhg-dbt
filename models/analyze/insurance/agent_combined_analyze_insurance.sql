 {{  config(alias='agent_combined', database='analyze', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('agent_combined_integrate_insurance')  }}