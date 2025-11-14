 {{  config(alias='processedcommissions_vc', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('processedcommissions_vc_analyze_investment')  }}