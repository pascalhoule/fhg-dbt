{{  config(alias='transactiontypes', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('transactiontypes_clean_investment')  }}