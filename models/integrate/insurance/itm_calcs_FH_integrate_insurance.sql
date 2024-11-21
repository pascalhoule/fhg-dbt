{{ config(alias = 'itm_calcs_FH', 
    materialized = 'view',
    database = 'integrate', 
    schema = 'insurance') }} 

    SELECT * from {{ ref('itm_calcs_FH_normalize_insurance') }}