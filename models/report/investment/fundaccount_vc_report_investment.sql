 {{  config(alias='fundaccount_vc', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundaccount_vc_analyze_investment')  }}