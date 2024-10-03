{{  config(alias='transactions_vc', database='report', schema='investment')  }} 


SELECT * 
  


from {{ ref ('transactions_vc_analyze_investment')  }}