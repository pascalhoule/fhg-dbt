 {{  config(alias='constants', database='normalize', schema='investment')  }} 


SELECT * 
  


from {{ ref ('constants_clean_investment')  }}