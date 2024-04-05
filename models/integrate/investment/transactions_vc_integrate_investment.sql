 {{  config(alias='transactions_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('transactions_vc_normalize_investment')  }}