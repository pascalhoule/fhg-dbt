{{			
    config (			
        materialized="view",			
        alias='aua', 			
        database='report', 			
        schema='segfund',
        grants = {'ownership': ['FH_READER']},			
    )			
}}

SELECT
    REPCODE,
    REPRESENTIATIVECODE,
    INSAGENTCODE,
    REPID,
    CODE,
    TRENDDATE::DATE AS TRENDDATE,
    FIRST_NAME,
    LAST_NAME,
    FUNDPRODUCT.FUNDPRODUCTCODE,
    FUNDPRODUCT.SPONSORID,
    SPONSOR.NAME,
    FUNDPRODUCT.FUNDID,
    FUNDPRODUCT.NAME,
    FUNDPRODUCT.LOADTYPE,
    FUNDPRODUCT.SUBTYPENAME,
    FUNDPRODUCT.SERVICEFEERATE,
    SUM(MARKETVALUE) AS TOTAL_MARKET_VALUE
FROM
    {{ ref('aua_vc_report_investment') }} AUA
    join {{ ref('representatives_vc_report_investment') }} REP ON AUA.REPCODE = REP.REPRESENTIATIVECODE
    join {{ ref('fundproducts_vc_report_investment') }} FUNDPRODUCT on AUA.FUNDPRODUCTCODE = FUNDPRODUCT.FUNDPRODUCTCODE
    JOIN {{ ref('sponsor_vc_report_investment') }} SPONSOR ON SPONSOR.SPONSORID = FUNDPRODUCT.SPONSORID
WHERE
    TRENDDATE::DATE >= '2025-01-01' 
group by
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16