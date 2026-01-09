{{  config(alias='representativeemail_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * 
  
FROM {{ ref ('representativeemail_vc_clean_investment')  }}