 {{  config(alias='state', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('state_normalize_investment')  }}