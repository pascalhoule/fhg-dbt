{{  config(alias='dealer_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('dealer_vc_clean_investment')  }}