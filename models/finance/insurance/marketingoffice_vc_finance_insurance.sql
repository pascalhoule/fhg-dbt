{{  config(
    alias='marketingoffice_vc', 
    database='finance', 
    schema='insurance', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('marketingoffice_vc_clean_insurance')  }}