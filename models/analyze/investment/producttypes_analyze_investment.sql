 {{  config(alias='producttypes', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('producttypes_integrate_investment')  }}