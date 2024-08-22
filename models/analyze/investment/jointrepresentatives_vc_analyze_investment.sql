{{  config(alias='jointrepresentatives_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('jointrepresentatives_vc_integrate_investment')  }}