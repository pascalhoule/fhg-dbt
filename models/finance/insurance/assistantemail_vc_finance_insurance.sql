{{  config(alias='assistantemail_vc', 
    database='finance', 
    schema='insurance', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('assistantemail_vc_clean_insurance')  }}

