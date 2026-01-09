{{  config(alias='transactiontypes_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('transactiontypes_vc_clean_investment')  }}