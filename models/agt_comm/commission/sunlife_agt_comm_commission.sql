{{ config(alias='sunlife', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view"
    ) 
}} 

SELECT * FROM {{ ref('sunlife_integrate_comm_extracts') }}