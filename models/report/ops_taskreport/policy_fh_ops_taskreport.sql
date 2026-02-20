{{		config (			
        materialized="view",			
        alias='policy', 			
        database='report', 			
        schema='ops_taskreport',
        tags="ops_taskreport"			
    )			}}	


    select *
    from {{ ref('policy_fh_normalize_insurance') }}


