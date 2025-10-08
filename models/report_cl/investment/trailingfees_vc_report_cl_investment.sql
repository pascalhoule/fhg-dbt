 {{  config(alias='trailingfees_vc', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('trailingfees_vc_analyze_investment')  }}