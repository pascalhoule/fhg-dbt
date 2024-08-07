{{			
    config (			
        materialized="view",			
        alias='broker', 			
        database='report', 			
        schema='in_the_mill',
        tags=["in_the_mill"]			
    )			
}}	

SELECT
    PARENTNODEID,
    AGENTCODE,
    AGENTNAME,
    COMPANYNAME,
    MGACODE,
    AGACODE,
    DATEOFBIRTH,
    PROVINCE,
    COMPANYPROVINCE,
    AGENTSTATUS,
    AGENTTYPE,
    LANGUAGEPREFERENCE,
    SERVICELEVEL,
    BROKERID,
    USERDEFINED2,
    MAP_SEGMENT,
    SEGMENTTAGWS,
    ELEVATED,
    PENDINGTERMINATION,
    TERMINATED,
    TRANSFERRINGOUT,
    RSC,
    BDC,
    BDC_MAP,
    SD
FROM
    {{ ref('broker_report_insurance') }}