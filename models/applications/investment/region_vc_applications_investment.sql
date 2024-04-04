 {{  config(alias='region_vc', database='applications', schema='investment', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('region_vc_analyze_investment')  }}