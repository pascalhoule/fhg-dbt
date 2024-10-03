 {{  config(alias='aua_vc', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('aua_vc_analyze_investment')  }}