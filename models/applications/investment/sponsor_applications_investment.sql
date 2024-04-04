 {{  config(alias='sponsor', database='applications', schema='investment', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('sponsor_analyze_investment')  }}