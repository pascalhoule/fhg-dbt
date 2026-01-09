{{  config(alias='purchases_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('purchases_vc_clean_investment')  }}