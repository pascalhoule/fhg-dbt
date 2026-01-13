{{  config(
    alias='hierarchycontract_vc', 
    database='finance', 
    schema='insurance', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('hierarchycontract_vc_clean_insurance')  }}