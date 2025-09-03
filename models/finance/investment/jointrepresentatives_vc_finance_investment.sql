{{  config(alias='jointrepresentatives_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('jointrepresentatives_vc_clean_investment')  }}