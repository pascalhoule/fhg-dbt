 {{  config(alias='registration_consultant', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('registration_normalize_investment_consultant')  }}