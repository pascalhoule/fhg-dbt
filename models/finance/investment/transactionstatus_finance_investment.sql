{{  config(alias='transactionstatus', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('transactionstatus_clean_investment')  }}