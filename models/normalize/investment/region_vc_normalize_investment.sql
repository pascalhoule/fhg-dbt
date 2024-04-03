 {{  config(alias='region_vc', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('region_vc_clean_investment')  }}
