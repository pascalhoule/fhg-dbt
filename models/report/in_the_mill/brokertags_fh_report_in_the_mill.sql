{{			
    config (			
        materialized="view",			
        alias='brokertags_fh', 			
        database='report', 			
        schema='in_the_mill',
        tags=["in_the_mill"]			
    )			
}}	

SELECT
    AGENTCODE,
    SPLIT_PART(TAGNAME, '/', 1) AS EN_TAGNAME,
    SPLIT_PART(TAGNAME, '/', 2) AS FR_TAGNAME,
    INHERITED,
    SOURCE
FROM
    {{ ref('brokertags_vc_report_insurance') }}