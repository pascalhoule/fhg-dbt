 {{  config(alias='policyclientlinking', database='integrate', schema='insurance')  }} 


SELECT * 
  


from {{ ref ('policyclientlinking_normalize_insurance')  }}