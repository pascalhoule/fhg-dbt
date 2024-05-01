 {{  config(alias='clients', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('clients_integrate_investment')  }}