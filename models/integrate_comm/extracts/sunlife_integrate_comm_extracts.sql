{{ config(alias='sunlife', 
    database='integrate_comm', 
    schema='extracts', 
    materialization = "view",
    grants = {'ownership': ['COMMISSION']},)  
}} 

SELECT
    'Sunlife' as "COMPANY",
    "BRANCH" as "DISTRIBUTOR CODE",
    CAST("FIELD FORCE ID" as VARCHAR) as "AGENT SELLING CODE",
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
    "COMM. TYPE",
    "COMM. SUBTYPE",      
    CASE
        WHEN TRIM("EIM DIVISION") = 'nan' THEN NULL
        ELSE TRIM("EIM DIVISION")
    END AS "DIVISION",    
    CASE
        WHEN TRIM("EIM LOB") = 'nan' THEN NULL
        ELSE TRIM("EIM LOB")
    END AS "LINE OF BUSINESS",      
    CASE
        WHEN TRIM("EIM PRODUCT") = 'nan' THEN NULL
        ELSE TRIM("EIM PRODUCT")
    END AS "PRODUCT",  
    CASE
        WHEN TRIM(CAST("BASIC PLAN CODE" AS VARCHAR)) = 'nan' THEN NULL
        ELSE TRIM(CAST("BASIC PLAN CODE" AS VARCHAR))
    END AS "BASIC PLAN CODE",
    "COMMISSION RATE",
    "STMT BASIS AMOUNT" as "STATEMENT BASIS AMOUNT",
    "AMOUNT",
    "SHARE" as "SHARE %",
    CAST("FROM/TO ID" as VARCHAR) as "FROM/TO ADVISOR",
    CASE
        WHEN TRIM("BUSINESS FIRST NAME") = 'nan' THEN NULL
        ELSE TRIM("BUSINESS FIRST NAME")
    END AS "BUSINESS FIRST NAME",    
    CASE
        WHEN TRIM("BUSINESS SURNAME") = 'nan' THEN NULL
        ELSE TRIM("BUSINESS SURNAME")
    END AS "BUSINESS SURNAME",
    CASE
        WHEN LEFT("COMM. TYPE", POSITION('-' in "COMM. TYPE") - 1) IN ('FYC', 'FOV') THEN 'First Year'
        WHEN LEFT("COMM. TYPE", POSITION('-' in "COMM. TYPE") - 1) IN ('REN', 'ROV') THEN 'Renewal'
        ELSE 'N/A'
    END AS "COMMISSION TAG",
    FH_FILENAME, 
    TO_TIMESTAMP(FH_TIMESTAMP) AS FH_TIMESTAMP
FROM {{ source('ren_comm', 'sunlife') }}