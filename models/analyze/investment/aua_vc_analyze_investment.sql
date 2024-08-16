 {{  config(alias='aua_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('aua_vc_integrate_investment')  }}