 {{  config(alias='branches_vc', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('branches_vc_clean_investment')  }}