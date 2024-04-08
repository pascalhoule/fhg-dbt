 {{  config(alias='transactions_vc', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('transactions_vc_clean_investment')  }}