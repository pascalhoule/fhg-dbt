 {{  config(alias='registration', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('registration_clean_investment') }}