{{  config(alias='transactiontypes', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('transactiontypes_clean_investment')  }}