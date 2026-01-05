{{  config(alias='region_fin_fh', 
    database='report', 
    schema='investment', 
    materialized = "view")  }} 

SELECT *
FROM {{ ref('region_fin_fh_analyze_investment') }}