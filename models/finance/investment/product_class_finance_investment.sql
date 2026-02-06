{{  config(alias='product_class', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * FROM {{ ref('product_class_analyze_investment') }}