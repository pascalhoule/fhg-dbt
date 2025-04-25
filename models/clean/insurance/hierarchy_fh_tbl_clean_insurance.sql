{{			
    config (			
        materialized="table",			
        alias='hierarchy_fh', 			
        database='clean', 			
        schema='insurance'			
    )			
}}

SELECT
    B.AGENTCODE,
    B.AGENTNAME,
    B.AGENTSTATUS,
    B.AGENTTYPE,
    B.BROKERID,
    BA.USERDEFINED2,
    H.PARENTNODEID,
    H.NODEID,
    H.NODENAME,
    H.HIERARCHYPATH,
    H.HIERARCHYPATHNAME,
    TRIM(SPLIT_PART(H.HIERARCHYPATHNAME, '>>', 1)) AS REGION,
    TRIM(SPLIT_PART(H.HIERARCHYPATHNAME, '>>', 2)) AS MARKET,
    TRIM(SPLIT_PART(H.HIERARCHYPATHNAME, '>>', 3)) AS LOCATION,
    CASE
        WHEN SPLIT_PART(H.HIERARCHYPATHNAME, '>>', 4) = ''
        and HIERARCHYLEVEL not in (1, 2) THEN CONCAT(B.AGENTNAME, '-FIRM')
        ELSE SPLIT_PART(H.HIERARCHYPATHNAME, '>>', 4)
    END AS FIRM
FROM
    {{ ref('broker_vc_clean_insurance') }} B
    LEFT JOIN {{ ref('brokeradvanced_vc_clean_insurance') }} BA on BA.AGENTCODE = B.AGENTCODE
    LEFT JOIN {{ ref('recursive_hierarchy_normalize_insurance') }} H on CONCAT('^', B.PARENTNODEID, '^') = H.NODEID
WHERE
    --B.AGENTTYPE <> 'Corporate' AND
    (
        BA.USERDEFINED2 LIKE '3268%'
        or BA.USERDEFINED2 LIKE '3162%'
    )
ORDER BY
    BA.USERDEFINED2