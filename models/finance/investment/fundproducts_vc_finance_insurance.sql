{{  config(alias='fundproducts_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('fundproducts_vc_clean_investment')  }}