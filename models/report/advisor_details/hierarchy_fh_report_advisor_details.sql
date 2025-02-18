{{		config (			
        materialized="view",			
        alias='hierarchy_fh', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    )			}}	


    SELECT
        *
    FROM
        {{ ref('hierarchy_fh_report_insurance') }}
    