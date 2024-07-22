{{  config(alias='plancategory_map', 
database='finance', 
schema='queries', 
materialization = "view")  }} 

SELECT * 
FROM {{ source('dimensions', 'plancategory_map') }}