 {{  config(alias='jointrepresentatives_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('jointrepresentatives_vc_normalize_investment')  }}