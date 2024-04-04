{{  config(alias='representatives_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_vc_integrate_investment')  }}