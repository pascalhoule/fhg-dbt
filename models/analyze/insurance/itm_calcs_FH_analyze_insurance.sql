{{ config(alias = 'itm_calcs_FH', 
    materialized = 'view',
    database = 'analyze', 
    schema = 'insurance') }} 

    SELECT * from {{ ref('itm_calcs_FH_integrate_insurance') }}