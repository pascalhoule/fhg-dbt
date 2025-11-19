{{		config (			
        materialized="view",			
        alias='broker', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    )			}}	

SELECT
    *
FROM
    {{ ref ('broker_fh_cl_report_insurance') }}