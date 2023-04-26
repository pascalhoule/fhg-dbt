 {{  config(alias='clients', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('clients_normalize_investment')  }}