{{			
    config (			
        materialized="view",			
        alias='policytags_fh', 			
        database='normalize', 			
        schema='insurance',
        tags=["in_the_mill"]			
    )			
}}	

SELECT
    POL.POLICYCODE,
    INDEX,
    SPLIT_PART(REPLACE(VALUE, '"', ''), '/', 1) AS TAGEN,
    CASE
        WHEN SPLIT_PART(REPLACE(VALUE, '"', ''), '/', 2) = '' THEN TAGEN
        ELSE SPLIT_PART(REPLACE(VALUE, '"', ''), '/', 2)
    END AS TAGFR
FROM
    {{ ref('policytags_vc_normalize_insurance') }} POL,
    LATERAL FLATTEN(INPUT => TAGNAME_ARRAY)