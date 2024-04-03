 {{  config(alias='region_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('region_vc_normalize_investment')  }}