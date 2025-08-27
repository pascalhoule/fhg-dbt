{{  config(alias='brokerassistant_vc', 
    database='finance', 
    schema='insurance', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('brokerassistant_vc_clean_insurance')  }}