{{  config(
    materialized = 'view',
    alias='agent_firm_map', 
    database='report_cl', 
    schema='firms',
    grants = {'ownership': ['BI_DEV']},	)  }} 

SELECT
    B.AGENTCODE,
    AGENTNAME,
    COMPANYNAME,
    AGENTSTATUS,
    AGENTTYPE,
    H.*,
    CASE
        WHEN HIERARCHYLEVEL = 7 THEN SPLIT_PART(HIERARCHYPATHNAME, '>>', 4)
        WHEN HIERARCHYLEVEL = 6
        AND (
            CONTAINS(HIERARCHYPATHNAME, 'MAP - Freedom')
            OR CONTAINS(HIERARCHYPATHNAME, 'MAP - WISE')
            OR CONTAINS(HIERARCHYPATHNAME, 'People Corporation')
        ) THEN SPLIT_PART(HIERARCHYPATHNAME, '>>', 4)
        WHEN HIERARCHYLEVEL = 5
        AND NOT (
            CONTAINS(HIERARCHYPATHNAME, 'MAP - Freedom')
            OR CONTAINS(HIERARCHYPATHNAME, 'MAP - WISE')
            OR CONTAINS(HIERARCHYPATHNAME, 'People Corporation')
        ) THEN SPLIT_PART(HIERARCHYPATHNAME, '>>', 4)
        WHEN HIERARCHYLEVEL = 6 THEN SPLIT_PART(HIERARCHYPATHNAME, '>>', 4) -- this will work because the CASE will exit after the first condition for MAP cases
        ELSE 'No Branch Name'
    END AS BRANCH_NAME,
    CASE
        WHEN HIERARCHYLEVEL = 7 THEN SPLIT_PART(HIERARCHYPATHNAME, '>>', 6)
        WHEN HIERARCHYLEVEL = 6
        AND (
            CONTAINS(HIERARCHYPATHNAME, 'MAP - Freedom')
            OR CONTAINS(HIERARCHYPATHNAME, 'MAP - WISE')
            OR CONTAINS(HIERARCHYPATHNAME, 'People Corporation')
        ) THEN SPLIT_PART(HIERARCHYPATHNAME, '>>', 6)
        WHEN HIERARCHYLEVEL = 5
        AND NOT (
            CONTAINS(HIERARCHYPATHNAME, 'MAP - Freedom')
            OR CONTAINS(HIERARCHYPATHNAME, 'MAP - WISE')
            OR CONTAINS(HIERARCHYPATHNAME, 'People Corporation')
        ) THEN SPLIT_PART(HIERARCHYPATHNAME, '>>', 5)
        WHEN HIERARCHYLEVEL = 6 THEN SPLIT_PART(HIERARCHYPATHNAME, '>>', 5) -- this will work because the CASE will exit after the first condition for MAP cases
        ELSE 'No Firm Name'
    END AS FIRM_NAME,
FROM
    {{ ref('broker_vc_report_cl_insurance') }} b
    LEFT JOIN {{ ref('recursive_hierarchy_report_cl_insurance') }} h ON CONCAT('^', B.PARENTNODEID, '^') = H.NODEID
WHERE
    AGENTTYPE NOT IN ('Corporate', 'Financial Horizons')
    and AGENTSTATUS = 'Active'