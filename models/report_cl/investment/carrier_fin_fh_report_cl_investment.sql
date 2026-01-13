{{  config(alias='carrier_fin_fh', 
    database='report_cl', 
    schema='investment', 
    materialized = "view")  }} 

SELECT *
FROM {{ ref('carrier_fin_fh_analyze_investment') }}