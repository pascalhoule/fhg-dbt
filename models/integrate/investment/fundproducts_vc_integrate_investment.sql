 {{  config(alias='fundproducts_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundproducts_vc_normalize_investment')  }}