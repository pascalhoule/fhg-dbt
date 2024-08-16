 {{  config(alias='aua_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('aua_vc_normalize_investment')  }}