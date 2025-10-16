{{  config(alias='transactions_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('transactions_vc_clean_investment')  }}