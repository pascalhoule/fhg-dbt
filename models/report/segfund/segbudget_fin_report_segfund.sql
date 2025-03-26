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
    ADVISORID,
    ADVISORNAME,
    '' AS BRANCH,
    REGION,
    REVTRANSTYPE AS TRANS_TYPE,
    PERIOD AS MTH,
    YEAR1 as YR,
    AMOUNT AS AMOUNT
FROM
    {{ ref('segbudget_fin') }}
UNION ALL
SELECT
    'FH' as BUSINESS_STREAM,
    BROKERID::VARCHAR AS ADVISORID,
    BROKERNAME AS ADVISORNAME,
    BRANCH,
    REGION,
    'Deposit' AS TRANS_TYPE,
    PERIOD AS MTH,
    YEAR1 AS YR,
    DEPOSIT AS AMOUNT
FROM
    {{ ref('fh_depositbudget_fin') }}
UNION ALL
SELECT
    'FH' as BUSINESS_STREAM,
    BROKERID::VARCHAR() AS ADVISORID,
    BROKERNAME AS ADVISORNAME,
    BRANCH,
    REGION,
    'Withdrawal' AS TRANS_TYPE,
    PERIOD AS MTH,
    YEAR1 AS YR,
    AMOUNT AS AMOUNT
FROM
    {{ ref('fh_withdrawalbudget_fin') }}