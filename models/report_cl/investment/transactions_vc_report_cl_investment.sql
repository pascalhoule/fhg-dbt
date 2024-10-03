{{  config(alias='transactions_vc', database='report_cl', schema='investment')  }} 


SELECT * 
  


from {{ ref ('transactions_vc_analyze_investment')  }}