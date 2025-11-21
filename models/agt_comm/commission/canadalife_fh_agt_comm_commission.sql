{{ config(alias='canadalife_fh', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view"
)
}} 

SELECT * FROM {{ ref('canadalife_integrate_comm_extracts') }}

WHERE ("BUSINESS SURNAME" ILIKE '%Horizons%' OR "BUSINESS FIRST NAME" ILIKE '%Horizons%')