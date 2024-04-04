{{  config(alias='sponsor', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('sponsor_integrate_investment')  }}