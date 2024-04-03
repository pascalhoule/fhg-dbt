 {{  config(alias='fundproducts_vc', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('fundproducts_vc_clean_investment')  }}