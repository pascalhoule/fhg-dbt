 {{  config(alias='sponsor', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('sponsor_normalize_investment')  }}