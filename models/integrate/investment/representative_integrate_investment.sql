{{  config(alias='representatives', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('representatives_normalize_investment')  }}