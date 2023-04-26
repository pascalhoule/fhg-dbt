 {{  config(alias='registration', database='integrate', schema='investment')  }} 


SELECT * 
  


from {{ ref ('registration_normalize_investment')  }}