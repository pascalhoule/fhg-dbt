{{  config(alias='product_class', 
    database='clean', 
    schema='investment')  }} 

SELECT *
FROM {{ source ('investment', 'product_class')  }} 