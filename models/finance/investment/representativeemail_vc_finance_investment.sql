{{  config(alias='representativeemail_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('representativeemail_vc_clean_investment')  }}