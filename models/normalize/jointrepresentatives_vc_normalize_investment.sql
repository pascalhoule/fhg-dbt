{{  config(alias='jointrepresentatives_vc', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('jointrepresentatives_vc_clean_investment')  }}