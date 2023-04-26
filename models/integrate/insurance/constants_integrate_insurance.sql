 {{  config(alias='constants', database='integrate', schema='insurance')  }} 
 


SELECT * 
  


from {{ ref ('constants_normalize_insurance')  }}