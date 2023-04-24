 {{  config(alias='beneficiaries', database='normalize', schema='investment')  }} 


SELECT * 
  


from {{ ref ('beneficiaries_clean_investment')  }}