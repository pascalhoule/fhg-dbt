{{  config(alias='processedcommissions_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('processedcommissions_vc_clean_investment')  }}