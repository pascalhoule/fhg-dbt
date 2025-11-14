{{ config(alias='manulife_fh', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view" 
)
}} 

SELECT 

    *

FROM {{ ref('manulife_integrate_comm_extracts') }}

WHERE ("BUSINESS SURNAME" ILIKE '%Horizons%' OR "BUSINESS FIRST NAME" ILIKE '%Horizons%')