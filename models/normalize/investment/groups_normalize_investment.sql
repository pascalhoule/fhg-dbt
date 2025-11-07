 {{  config(
    alias='groups', 
    database='normalize', 
    schema='investment')  }} 

SELECT * FROM {{ ref('groups_clean_investment') }}