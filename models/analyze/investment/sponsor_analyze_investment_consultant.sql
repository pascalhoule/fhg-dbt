{{  config(alias='sponsor_consultant', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('sponsor_integrate_investment_consultant')  }}