{{  config(alias='transactions_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('transactions_vc_clean_investment')  }}