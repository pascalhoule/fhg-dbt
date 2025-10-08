{{  config(alias='transactionstatus', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('transactionstatus_clean_investment')  }}