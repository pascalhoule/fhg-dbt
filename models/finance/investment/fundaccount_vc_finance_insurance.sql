{{  config(alias='fundaccount_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('fundaccount_vc_clean_investment')  }}