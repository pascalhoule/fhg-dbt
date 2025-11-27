{{ config(alias='empire', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view"
)
}} 

SELECT * FROM {{ ref('empire_integrate_comm_extracts') }}