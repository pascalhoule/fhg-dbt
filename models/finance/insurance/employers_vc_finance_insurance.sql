{{  config(
    alias='employers_vc', 
    database='finance', 
    schema='insurance', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('employers_vc_clean_insurance')  }}