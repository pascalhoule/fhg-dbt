 {{  config(alias='sponsors_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('sponsors_vc_normalize_investment')  }}