{{  config(alias='processedcommissions_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('processedcommissions_vc_clean_investment')  }}