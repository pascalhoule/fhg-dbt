{{  config(
    alias='entitytags_vc', 
    database='finance', 
    schema='insurance', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('entitytags_vc_clean_insurance')  }}