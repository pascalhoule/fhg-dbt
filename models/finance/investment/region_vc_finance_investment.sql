{{  config(alias='region_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('region_vc_clean_investment')  }}