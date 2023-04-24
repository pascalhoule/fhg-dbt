 {{  config(alias='ic', database='integrate', schema='insurance')  }} 
 


SELECT * 
  


from {{ ref ('ic_normalize_insurance')  }}