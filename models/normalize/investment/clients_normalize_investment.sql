{{  config(alias='clients', database='normalize', schema='investment')  }} 


SELECT * 
  

FROM {{ ref ('clients_clean_investment')  }}