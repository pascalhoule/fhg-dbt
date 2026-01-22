{{ config(
    alias='equitable', 
    database='integrate_comm', 
    schema='extracts', 
    materialized='view',
    grants={'ownership': ['COMMISSION']},
) }} 

SELECT
    'Equitable' AS "COMPANY",
        MGA,
        "BRANCH" AS "DISTRIBUTOR CODE",
        "AGENT" AS "AGENT SELLING CODE",
        CASE
            WHEN TRIM("POLICY") = 'nan' THEN NULL
            ELSE TRIM("POLICY")
        END AS "POLICY",
        CASE
            WHEN TRIM("OWNER LAST NAME") = 'nan' THEN NULL
            ELSE TRIM("OWNER LAST NAME")
        END AS "LAST NAME",
        DATE("GEN DATE") AS "GENERATION DATE",
        "DESC" AS "COMMISSION TYPE",
        CASE
            WHEN TRIM(CAST("PLAN CODE" AS VARCHAR)) = 'nan' THEN NULL
            ELSE TRIM(CAST("PLAN CODE" AS VARCHAR))
        END AS "BASIC PLAN CODE",
        "COMMISSIONABLE PREMIUM",
        "COMM RATE" AS "COMMISSION RATE",
        "COMP AMT" AS "AMOUNT",
        "COMM SHARE" AS "SHARE %",
        NULL AS "BUSINESS FIRST NAME",  
        CASE
            WHEN TRIM("AGENT NAME") = 'nan' THEN NULL
            ELSE TRIM("AGENT NAME")
        END AS "BUSINESS SURNAME",
        CASE
            WHEN TRIM("DESC") IN ('Annuity FYC', 'Annuity Override', 'Life FYC', 'Life Override') THEN 'First Year'
            WHEN TRIM("DESC") IN ('Life Renewal', 'Life Renewal Override') THEN 'Renewal'
            WHEN TRIM("DESC") IN ('Debit Balance Recovery', 'Deductions') THEN 'Debt'
            WHEN TRIM("DESC") IN ('Group Renewal') THEN 'Group'
            WHEN TRIM("DESC") IN ('Misc') THEN 'Other'
            ELSE 'N/A'
        END AS "COMMISSION TAG",
        "POLICY COUNT",
        FH_FILENAME,
        FH_TIMESTAMP
FROM {{ source('ren_comm', 'equitable') }}