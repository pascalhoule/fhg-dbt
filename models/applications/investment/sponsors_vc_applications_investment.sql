 {{  config(alias='sponsors_vc', database='applications', schema='investment', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('sponsors_vc_analyze_investment')  }}