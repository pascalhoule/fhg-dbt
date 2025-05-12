{{  config(alias='total_contest_credits', 
database='clean', 
schema='for_share')  }} 

SELECT *
FROM {{ source('contest', 'total_contest_credits') }}