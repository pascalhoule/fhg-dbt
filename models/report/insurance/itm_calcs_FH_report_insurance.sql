{{ config(alias = 'itm_calcs_FH', 
    materialized = 'view',
    database = 'report', 
    schema = 'insurance') }} 

    SELECT * from {{ ref('itm_calcs_FH_analyze_insurance') }}