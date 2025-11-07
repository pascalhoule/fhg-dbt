{{  config(
    alias='groups', 
    database='analyze', 
    schema='investment')  }} 

SELECT * FROM {{ ref('groups_integrate_investment') }}