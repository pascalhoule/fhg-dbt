 {{  config(alias='fundaccount_vc', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('fundaccount_vc_clean_investment')  }}