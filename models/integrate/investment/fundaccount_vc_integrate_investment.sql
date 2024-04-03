 {{  config(alias='fundaccount_vc', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundaccount_vc_normalize_investment')  }}