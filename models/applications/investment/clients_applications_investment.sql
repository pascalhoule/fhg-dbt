 {{  config(alias='clients', database='applications', schema='investment', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('clients_analyze_investment')  }}