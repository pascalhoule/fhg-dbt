{{  config(alias='aga', database='normalize', schema='insurance')  }} 

SELECT * 
  
FROM {{ ref ('aga_clean_insurance')  }}