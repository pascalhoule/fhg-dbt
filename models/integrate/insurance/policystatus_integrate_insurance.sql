 {{  config(alias='policystatus', database='integrate', schema='insurance')  }} 
 


SELECT * 
  


from {{ ref ('policystatus_normalize_insurance')  }}