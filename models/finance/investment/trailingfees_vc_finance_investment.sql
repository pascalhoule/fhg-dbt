{{  config(alias='trailingfees_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('trailingfees_vc_clean_investment')  }}