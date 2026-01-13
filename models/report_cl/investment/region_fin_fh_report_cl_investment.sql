{{  config(alias='region_fin_fh', 
    database='report_cl', 
    schema='investment', 
    materialized = "view")  }} 

SELECT *
FROM {{ ref('region_fin_fh_analyze_investment') }}