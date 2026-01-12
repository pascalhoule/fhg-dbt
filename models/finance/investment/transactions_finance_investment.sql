{{  config(alias='transactions', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('transactions_clean_investment')  }}