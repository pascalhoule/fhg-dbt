{{  config(alias='users_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('users_vc_clean_investment')  }}