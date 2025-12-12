{{  config(alias='processedtrailfees_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('processedtrailfees_vc_clean_investment')  }}