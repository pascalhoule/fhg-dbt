{{  config(alias='representatives', database='normalize', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_clean_investment')  }}