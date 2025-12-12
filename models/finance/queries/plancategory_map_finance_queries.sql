{{  config(alias='plancategory_map', 
database='finance', 
schema='queries', 
materialized = "view")  }} 

SELECT * 
FROM {{ source('dimensions', 'plancategory_map') }}