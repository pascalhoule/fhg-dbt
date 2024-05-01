{{  config(alias='representatives', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_integrate_investment')  }}