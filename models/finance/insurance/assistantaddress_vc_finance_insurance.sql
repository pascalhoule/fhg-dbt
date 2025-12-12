{{  config(alias='assistantaddress_vc', 
    database='finance', 
    schema='insurance', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('assistantaddress_vc_clean_insurance')  }}