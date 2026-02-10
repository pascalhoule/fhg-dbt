{{		config (			
        materialized="view",			
        alias='policytasks', 			
        database='report', 			
        schema='ops_taskreport',
        tags="ops_taskreport"			
    )			}}	

SELECT
    *
FROM
    {{ ref('policytasks_vc_clean_insurance') }}