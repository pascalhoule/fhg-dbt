 {{  config(alias='aua_vc_me', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('aua_vc_me_integrate_investment')  }}