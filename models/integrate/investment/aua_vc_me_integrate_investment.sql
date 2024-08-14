 {{  config(alias='aua_vc_me', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('aua_vc_me_normalize_investment')  }}