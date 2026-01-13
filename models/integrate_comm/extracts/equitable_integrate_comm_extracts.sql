{{ config(
    alias='equitable', 
    database='integrate_comm', 
    schema='extracts', 
    materialized='view',
    grants={'ownership': ['COMMISSION']},
) }} 

SELECT
    TRIM(MGA) AS MGA, 
    DATE("GEN DATE") AS GEN_DATE,
    TRIM(POLICY) AS POLICY,
    TRIM(BRANCH) AS BRANCH,
    TRIM("OWNER LAST NAME") AS OWNER_LAST_NAME,
    TRIM("PLAN CODE") AS PLAN_CODE,
    AGENT,
    TRIM("AGENT NAME") AS AGENT_NAME,
    TRIM(DESC) AS DESCRIPTION,
    "COMP AMT" AS COMP_AMT,
    "COMM SHARE" AS COMM_SHARE,
    "COMM RATE" AS COMM_RATE,
    "COMMISSIONABLE PREMIUM" AS COMMISSIONABLE_PREMIUM,
    "POLICY COUNT" AS POLICY_COUNT,
    FH_FILENAME, 
    TO_TIMESTAMP(FH_TIMESTAMP) AS FH_TIMESTAMP
FROM {{ source('ren_comm', 'equitable') }}