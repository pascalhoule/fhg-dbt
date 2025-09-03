{{  config(alias='trailingfees_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('trailingfees_vc_clean_investment')  }}