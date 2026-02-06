{{  config(
    alias='fund_products', 
    database='analyze', 
    schema='investment')  }} 

SELECT * FROM {{ ref('fund_products_integrate_investment') }}