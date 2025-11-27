{{  config(alias='client_vc', database='normalize', schema='insurance')  }} 


SELECT * 
  
FROM {{ ref ('client_vc_clean_insurance')  }}