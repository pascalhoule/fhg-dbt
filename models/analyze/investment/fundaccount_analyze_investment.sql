{{  config(alias='fundaccount', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundaccount_integrate_investment') }}