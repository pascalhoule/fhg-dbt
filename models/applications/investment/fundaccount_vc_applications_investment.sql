 {{  config(alias='fundaccount_vc', database='applications', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundaccount_vc_analyze_investment')  }}