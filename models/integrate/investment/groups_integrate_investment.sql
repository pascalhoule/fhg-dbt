{{  config(
    alias='groups', 
    database='integrate', 
    schema='investment')  }} 

SELECT * FROM {{ ref('groups_normalize_investment') }}