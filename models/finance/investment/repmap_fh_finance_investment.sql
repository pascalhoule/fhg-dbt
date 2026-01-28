{{  config(alias='repmap_fh', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT * FROM {{ ref('repmap_fh_normalize_investment') }}