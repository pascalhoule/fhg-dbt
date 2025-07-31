{{ config(alias='manulife', 
    database='agt_comm', 
    schema='commission', 
    materialization = "view",
    grants = {'ownership': ['COMMISSION']},)  
}} 

SELECT * FROM {{ ref('manulife_integrate_comm_extracts') }}