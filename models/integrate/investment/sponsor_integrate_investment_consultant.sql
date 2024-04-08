 {{  config(alias='sponsor_consultant', database='integrate', schema='investment')  }} 


SELECT * 
  

from {{ ref ('sponsor_normalize_investment_consultant')  }}