 {{  config(alias='branches_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('branches_vc_normalize_investment')  }}