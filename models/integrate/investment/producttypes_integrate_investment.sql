 {{  config(alias='producttypes', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('producttypes_normalize_investment')  }}