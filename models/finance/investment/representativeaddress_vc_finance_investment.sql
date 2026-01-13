{{  config(alias='representativeaddress_vc', database='finance', schema='investment', materialized = "view")  }} 

SELECT * 
  
from {{ ref ('representativeaddress_vc_clean_investment')  }}