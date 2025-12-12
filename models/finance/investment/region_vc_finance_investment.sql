{{  config(alias='region_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('region_vc_clean_investment')  }}