 {{  config(alias='product_class', database='normalize', schema='investment')  }} 

 SELECT * FROM {{ ref('product_class_investment_clean') }}