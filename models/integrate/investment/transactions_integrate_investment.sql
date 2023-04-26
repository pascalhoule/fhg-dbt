 {{  config(alias='transactions', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('transactions_normalize_investment')  }}