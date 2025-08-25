{{ config(alias='canadalife', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view"
)
}} 

SELECT * FROM {{ ref('canadalife_integrate_comm_extracts') }}