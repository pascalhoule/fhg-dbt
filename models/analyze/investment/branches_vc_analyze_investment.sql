 {{  config(alias='branches_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('branches_vc_integrate_investment')  }}