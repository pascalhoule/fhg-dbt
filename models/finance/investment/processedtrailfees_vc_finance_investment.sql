{{  config(alias='processedtrailfees_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('processedtrailfees_vc_clean_investment')  }}