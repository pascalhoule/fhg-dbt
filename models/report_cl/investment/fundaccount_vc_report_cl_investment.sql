 {{  config(alias='fundaccount_vc', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('fundaccount_vc_analyze_investment')  }}