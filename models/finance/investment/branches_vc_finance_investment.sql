{{  config(alias='branches_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('branches_vc_clean_investment')  }}