{{  config(
    alias='fund_products', 
    database='report_cl', 
    schema='investment')  }} 

SELECT * FROM {{ ref('fund_products_analyze_investments') }}