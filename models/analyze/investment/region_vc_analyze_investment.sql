 {{  config(alias='region_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('region_vc_integrate_investment')  }}