 {{  config(alias='fundaccount_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundaccount_vc_integrate_investment')  }}