{{  config(
    alias='fund_products', 
    database='finance', 
    schema='investment')  }} 

SELECT * FROM {{ ref('fund_products_analyze_investments') }}