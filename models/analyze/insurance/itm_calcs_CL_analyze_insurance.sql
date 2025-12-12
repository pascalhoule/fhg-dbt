{{ config(alias = 'itm_calcs_CL', 
    materialized = 'view',
    database = 'analyze', 
    schema = 'insurance') }} 

    SELECT * from {{ ref('itm_calcs_CL_integrate_insurance') }}