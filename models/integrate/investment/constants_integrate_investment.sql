 {{  config(alias='constants', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('constants_normalize_investment')  }}