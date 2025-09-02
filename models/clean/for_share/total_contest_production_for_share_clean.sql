{{  config(alias='total_contest_production', 
database='clean', 
schema='for_share')  }} 

SELECT *
FROM {{ source('contest', 'total_contest_production') }}