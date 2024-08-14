{{  config(alias='aua_vc_me', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('aua_vc_me_clean_investment')  }}