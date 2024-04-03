 {{  config(alias='sponsors_vc', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('sponsors_vc_clean_investment') }}