 {{  config(alias='registration', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('registration_integrate_investment')  }}