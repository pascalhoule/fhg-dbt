 {{  config(alias='branches_vc', database='applications', schema='investment', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('branches_vc_analyze_investment')  }}