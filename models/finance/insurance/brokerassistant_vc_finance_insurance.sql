{{  config(alias='brokerassistant_vc', 
    database='finance', 
    schema='insurance', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('brokerassistant_vc_clean_insurance')  }}