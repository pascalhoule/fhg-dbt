{{  config(alias='product_class', 
    database='report_cl', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * FROM {{ ref('product_class_analyze_investment') }}