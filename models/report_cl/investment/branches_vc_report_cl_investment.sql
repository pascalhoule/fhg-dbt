 {{  config(alias='branches_vc', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('branches_vc_analyze_investment')  }}