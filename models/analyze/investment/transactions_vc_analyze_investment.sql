 {{  config(alias='transactions_vc', database='analyze', schema='investment')  }} 


SELECT * 
  


from {{ ref ('transactions_vc_integrate_investment')  }}