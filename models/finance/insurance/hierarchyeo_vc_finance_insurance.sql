{{  config(
    alias='hierarchyeo_vc', 
    database='finance', 
    schema='insurance', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('hierarchyeo_vc_clean_insurance')  }}