{{  config(alias='representatives_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('representatives_vc_clean_investment')  }}