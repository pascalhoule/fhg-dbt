 {{  config(alias='fundaccount', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundaccount_normalize_investment')  }}