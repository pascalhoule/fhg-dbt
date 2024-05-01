 {{  config(alias='clients_consultant', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('clients_normalize_investment_consultant')  }}