 {{  config(alias='product_class', database='integrate', schema='investment')  }} 

 SELECT * FROM {{ ref('product_class_normalize_investment') }}