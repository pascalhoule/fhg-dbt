{{  config(alias='transactions_vc', database='applications', schema='investment', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('transactions_vc_analyze_investment')  }}