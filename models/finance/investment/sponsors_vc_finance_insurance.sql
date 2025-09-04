{{  config(alias='sponsors_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('sponsors_vc_clean_investment')  }}