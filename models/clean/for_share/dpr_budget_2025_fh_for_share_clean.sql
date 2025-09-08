{{  config(alias='budget_2025_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ source('budget_2025', 'budget_2025') }}