{{  config(alias='purchases_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('purchases_vc_clean_investment')  }}