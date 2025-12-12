{{ config(alias = 'itm_calcs_CL', 
    materialized = 'view',
    database = 'report', 
    schema = 'insurance') }} 

    SELECT * FROM {{ ref('itm_calcs_CL_analyze_insurance') }}