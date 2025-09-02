{{  config(alias='aua_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('aua_vc_clean_investment')  }}