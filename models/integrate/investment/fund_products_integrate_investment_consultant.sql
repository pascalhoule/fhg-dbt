 {{  config(alias='fund_products_consultant', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fund_products_normalize_investment_consultant')  }}