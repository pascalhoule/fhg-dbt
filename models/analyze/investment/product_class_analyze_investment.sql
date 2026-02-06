 {{  config(alias='product_class', database='analyze', schema='investment')  }} 

 SELECT * FROM {{ ref('product_class_integrate_investment') }}