{{ config( 
    alias='__base_seg_aua_for_contest', 
    database='agt_comm', 
    schema='contest', 
    materialized = "view" ) }}

SELECT
    JREP.INDIVIDUALREPRESENTATIVECODE AS REPCODE,
    JREP.SHARE,
    FUNDPRODUCTCODE,
    TRENDDATE,
    MARKETVALUE * (JREP.SHARE / 100) AS MARKETVALUE,
FROM
    {{ ref('aua_vc_agt_comm_investment') }} AUA
    JOIN {{ ref('jointrepresentatives_vc_agt_comm_investment') }} JREP on AUA.REPCODE = JREP.JOINTREPRESENTATIVECODE
UNION
SELECT
    REPCODE,
    JREP.SHARE,
    FUNDPRODUCTCODE,
    TRENDDATE,
    MARKETVALUE
FROM
    {{ ref('aua_vc_agt_comm_investment') }} AUA
    LEFT JOIN {{ ref('jointrepresentatives_vc_agt_comm_investment') }} JREP ON AUA.REPCODE = JREP.JOINTREPRESENTATIVECODE
WHERE
    JREP.SHARE IS NULL