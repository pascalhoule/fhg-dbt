{{  config(alias='fundproducts_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('fundproducts_vc_clean_investment')  }}