{{  config(
    alias='entitytags_vc', 
    database='finance', 
    schema='insurance', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('entitytags_vc_clean_insurance')  }}