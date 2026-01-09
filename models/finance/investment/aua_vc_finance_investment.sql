{{  config(alias='aua_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('aua_vc_clean_investment')  }}