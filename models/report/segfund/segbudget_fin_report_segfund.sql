{{			
    config (			
        materialized="view",			
        alias='segbudget_fin', 			
        database='report', 			
        schema='segfund',
        grants = {'ownership': ['FH_READER']},			
    )			
}}

SELECT
    'CL_DIRECT' as BUSINESS_STREAM,
    '0' as FH_BROKERID,
    ADVISORID::VARCHAR() as CL_ADVISORID,
    CASE WHEN AGENTCODE = 'Not Stitched' THEN 0 ELSE AGENTCODE END AS FH_AGENTCODE,
    CASE WHEN FH_UID = 'Not Stitched' THEN 0 ELSE FH_UID END AS FH_USERDEFINED2,
    ADVISORNAME,
    REVTRANSTYPE AS TRANS_TYPE,
    '' AS LOAD_TYPE,
    PERIOD AS MTH,
    YEAR1 as YR,
    AMOUNT AS AMOUNT
FROM
    {{ ref('segbudget_fin') }}
UNION ALL
SELECT
    'FH' as BUSINESS_STREAM,
    BROKERID::VARCHAR AS FH_BROKERID,
    'U0' as CL_ADVISORID,
    AGENTCODE AS FH_AGENTCODE,
    '0' AS FH_USERDEFINED2,
    BROKERNAME AS ADVISORNAME,
    'Deposit' AS TRANS_TYPE,
    MYLOADTYPE AS LOAD_TYPE,
    PERIOD AS MTH,
    YEAR1 AS YR,
    DEPOSIT AS AMOUNT
FROM
    {{ ref('fh_depositbudget_fin') }}
UNION ALL
SELECT
    'FH' as BUSINESS_STREAM,
    BROKERID::VARCHAR AS FH_BROKERID,
    'U0' as CL_ADVISORID,
    AGENTCODE AS FH_AGENTCODE,
    0 AS FH_USERDEFINED2,
    BROKERNAME AS ADVISORNAME,
    TRANSTYPE AS TRANS_TYPE,
    '' AS LOAD_TYPE,
    PERIOD AS MTH,
    YEAR1 AS YR,
    AMOUNT
FROM
    {{ ref('fh_withdrawalbudget_fin') }}