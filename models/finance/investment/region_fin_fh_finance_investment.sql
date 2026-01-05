{{  config(alias='region_fin_fh', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT *
FROM {{ source('norm', 'region_fin_fh') }}