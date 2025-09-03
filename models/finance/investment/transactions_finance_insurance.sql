{{  config(alias='transactions', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('transactions_clean_investment')  }}