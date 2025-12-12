{{ config(
    alias='canadalife', 
    database='integrate_comm', 
    schema='extracts', 
    materialized='view',
    grants={'ownership': ['COMMISSION']},
) }} 

SELECT
    'Canadalife' as "COMPANY",
    "DISTRIBUTOR CODE",
    "AGENT SELLING CODE",
    CASE
        WHEN TRIM("POLICY") = 'nan' THEN NULL
        ELSE TRIM("POLICY")
    END AS "POLICY",  
    CASE
        WHEN TRIM("FIRST NAME") = 'nan' THEN NULL
        ELSE TRIM("FIRST NAME")
    END AS "FIRST NAME",  
    CASE
        WHEN TRIM("LAST NAME") = 'nan' THEN NULL
        ELSE TRIM("LAST NAME")
    END AS "LAST NAME",
    DATE("GENERATION DATE") AS "GENERATION DATE",
    "COMMISSION TYPE",
    "COMMISSION SUBTYPE",    
    CASE
        WHEN TRIM("MODE") = 'nan' THEN NULL
        ELSE TRIM("MODE")
    END AS "MODE",  
    CASE
        WHEN TRIM("FASAT DIVISION") = 'nan' THEN NULL
        ELSE TRIM("FASAT DIVISION")
    END AS "DIVISION",    
    CASE
        WHEN TRIM("FASAT LINE OF BUSINESS") = 'nan' THEN NULL
        ELSE TRIM("FASAT LINE OF BUSINESS")
    END AS "LINE OF BUSINESS",      
    CASE
        WHEN TRIM("FASAT PRODUCT") = 'nan' THEN NULL
        ELSE TRIM("FASAT PRODUCT")
    END AS "PRODUCT",  
    CASE
        WHEN TRIM(CAST("BASIC PLAN CODE" AS VARCHAR)) = 'nan' THEN NULL
        ELSE TRIM(CAST("BASIC PLAN CODE" AS VARCHAR))
    END AS "BASIC PLAN CODE",
    "COMMISSION RATE",
    "STATEMENT BASIS AMOUNT",
    "AMOUNT",
    "SHARE PERCENT" as "SHARE %",
    "OVERRIDE COMMISSION AMOUNT",
    "COMMISSIONABLE PREMIUM",  
    CASE
        WHEN TRIM(COALESCE("FROM/TO ADVISOR"::TEXT, '')) = 'nan' THEN NULL
        ELSE TRIM(COALESCE("FROM/TO ADVISOR"::TEXT, ''))
    END AS "FROM/TO ADVISOR",  
    CASE
        WHEN TRIM(COALESCE("ORIGINAL ADVISOR"::TEXT, '')) = 'nan' THEN NULL
        ELSE TRIM(COALESCE("ORIGINAL ADVISOR"::TEXT, ''))
    END AS "ORIGINAL ADVISOR",    
    CASE
        WHEN TRIM("BUSINESS FIRST NAME") = 'nan' THEN NULL
        ELSE TRIM("BUSINESS FIRST NAME")
    END AS "BUSINESS FIRST NAME",    
    CASE
        WHEN TRIM("BUSINESS SURNAME") = 'nan' THEN NULL
        ELSE TRIM("BUSINESS SURNAME")
    END AS "BUSINESS SURNAME",
    DATE("PROCESSED DATE") AS "PROCESSED DATE",
    CASE
        WHEN TRIM("COMMISSION TYPE") IN ('FYC', 'FOV') THEN 'First Year'
        WHEN TRIM("COMMISSION TYPE") IN ('REN', 'ROV') THEN 'Renewal'
        WHEN TRIM("COMMISSION TYPE") IN ('DEB') THEN 'Debt'
        WHEN TRIM("COMMISSION TYPE") IN ('MSC') THEN 'Other'
        ELSE 'N/A'
    END AS "COMMISSION TAG",  
    FH_FILENAME, 
    FH_TIMESTAMP
FROM {{ source('ren_comm', 'canadalife') }}
