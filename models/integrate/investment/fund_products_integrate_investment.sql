 {{  config(alias='fund_products', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fund_products_normalize_investment')  }}