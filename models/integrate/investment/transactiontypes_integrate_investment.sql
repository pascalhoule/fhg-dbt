 {{  config(alias='transactiontypes', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('transactiontypes_normalize_investment')  }}