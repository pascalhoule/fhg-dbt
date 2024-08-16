{{  config(alias='aua_vc', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('aua_vc_clean_investment')  }}