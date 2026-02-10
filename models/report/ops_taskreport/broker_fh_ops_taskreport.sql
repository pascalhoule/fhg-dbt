{{		config (			
        materialized="view",			
        alias='broker', 			
        database='report', 			
        schema='ops_taskreport',
        tags="ops_taskreport"			
    )			}}	

SELECT
    *
FROM
    {{ ref('broker_fh_integrate_insurance') }}