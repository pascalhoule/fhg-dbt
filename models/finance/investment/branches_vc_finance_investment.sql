{{  config(alias='branches_vc', 
    database='finance', 
    schema='investment', 
    materialization = "view")  }} 

SELECT * 
  
FROM {{ ref ('branches_vc_clean_investment')  }}