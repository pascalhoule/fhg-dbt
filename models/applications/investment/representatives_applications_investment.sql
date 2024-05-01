 {{  config(alias='representatives', database='applications', schema='investment', materialized = "view")  }} 


SELECT * 
  


from {{ ref ('representatives_analyze_investment')  }}