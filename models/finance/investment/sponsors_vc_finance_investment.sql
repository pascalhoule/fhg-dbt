{{  config(alias='sponsors_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('sponsors_vc_clean_investment')  }}