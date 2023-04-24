 {{  config(alias='beneficiaries', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('beneficiaries_normalize_investment')  }}