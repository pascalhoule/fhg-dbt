 {{  config(alias='trailingfees_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('trailingfees_vc_integrate_investment')  }}