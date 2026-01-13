{{  config(
    alias='insuranceuser_vc', 
    database='finance', 
    schema='insurance', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('insuranceuser_vc_clean_insurance')  }}