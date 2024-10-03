{{  config(alias='jointrepresentatives_vc', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('jointrepresentatives_vc_analyze_investment')  }}