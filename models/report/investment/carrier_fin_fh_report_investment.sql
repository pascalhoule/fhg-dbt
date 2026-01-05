{{  config(alias='carrier_fin_fh', 
    database='report', 
    schema='investment', 
    materialized = "view")  }} 

SELECT *
FROM {{ ref('carrier_fin_fh_analyze_investment') }}