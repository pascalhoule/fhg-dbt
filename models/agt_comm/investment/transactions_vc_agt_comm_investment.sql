 {{  config(alias='transactions_vc', database='agt_comm', schema='investment')  }} 


SELECT * 
  


from {{ ref ('transactions_vc_analyze_investment')  }}