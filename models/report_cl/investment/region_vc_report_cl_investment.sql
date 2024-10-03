{{  config(alias='region_vc', database='report_cl', schema='investment', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('region_vc_analyze_investment')  }}