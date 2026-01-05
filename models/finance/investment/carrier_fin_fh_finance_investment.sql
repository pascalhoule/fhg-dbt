{{  config(alias='carrier_fin_fh', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT *
FROM {{ source('norm', 'carrier_fin_fh') }}