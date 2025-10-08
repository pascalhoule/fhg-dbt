{{  config(
    alias='transactions', 
    database='report_cl', 
    schema='investment')  }} 


SELECT * 
  


from {{ ref ('transactions_analyze_investment')  }}