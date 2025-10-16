{{  config(alias='trailingfees_vc', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('trailingfees_vc_clean_investment') }}