{{ config(
    alias='bmo', 
    database='integrate_comm', 
    schema='extracts', 
    materialized='view',
    grants={'ownership': ['COMMISSION']},
)  }} 

SELECT
    'BMO' as "COMPANY",
    REPLACE(MGA, 'BMO-', '') AS "DISTRIBUTOR CODE",                           // REMOVES THE FIRST FOUR DIGITS BMO-
    "FIELD FORCE ID" AS "AGENT SELLING CODE",
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
    DATE("GENERATION DT") AS "GENERATION DATE",    
    CASE
        WHEN TRIM("COMM TYPE") = 'nan' THEN NULL
        ELSE TRIM("COMM TYPE")
    END AS "COMMISSION TYPE",  
   CASE
        WHEN TRIM("COMM SUBTYPE") = 'nan' THEN NULL
        ELSE TRIM("COMM SUBTYPE")
    END AS "COMMISSION SUBTYPE", 
    CASE
        WHEN TRIM("DIVISION") = 'nan' THEN NULL
        ELSE TRIM("DIVISION")
    END AS "DIVISION",    
    CASE
        WHEN TRIM("LOB") = 'nan' THEN NULL
        ELSE TRIM("LOB")
    END AS "LINE OF BUSINESS",      
    CASE
        WHEN TRIM("PRODUCT") = 'nan' THEN NULL
        ELSE TRIM("PRODUCT")
    END AS "PRODUCT",  
    CASE
        WHEN TRIM(CAST("COV PLAN CODE" AS VARCHAR)) = 'nan' THEN NULL
        ELSE TRIM(CAST("COV PLAN CODE" AS VARCHAR))
    END AS "BASIC PLAN CODE",
    "COMMISSION RATE" AS "COMMISSION RATE",
    "STMT BASIS AMT" AS "STATEMENT BASIS AMOUNT",
    "AMOUNT",
    "SHARE" as "SHARE %",
    "COVERAGE",
    CASE
        WHEN TRIM(COALESCE("FROM/TO ID"::TEXT, '')) = 'nan' THEN NULL
        ELSE TRIM(COALESCE("FROM/TO ID"::TEXT, ''))
    END AS "FROM/TO ADVISOR",  
  
    CASE
        WHEN TRIM("BUSINESS FIRST NAME") = 'nan' THEN NULL
        ELSE TRIM("BUSINESS FIRST NAME")
    END AS "BUSINESS FIRST NAME",    
    CASE
        WHEN TRIM("BUSINESS SURNAME") = 'nan' THEN NULL
        ELSE TRIM("BUSINESS SURNAME")
    END AS "BUSINESS SURNAME",
    CASE
        WHEN TRIM("COMMISSION TYPE") IN ('FYC', 'FOV') THEN 'First Year'
        WHEN TRIM("COMMISSION TYPE") IN ('REN', 'ROV') THEN 'Renewal'
        WHEN TRIM("COMMISSION TYPE") IN ('DEB') THEN 'Debt'
        WHEN TRIM("COMMISSION TYPE") IN ('MSC') THEN 'Other'
        ELSE 'N/A'
    END AS "COMMISSION TAG",
    CASE
        WHEN TRIM("REGION") = 'nan' THEN NULL
        ELSE TRIM("REGION")
    END AS "REGION",    
   CASE
        WHEN TRIM("GENERATION TRANSACTION") = 'nan' THEN NULL
        ELSE TRIM("GENERATION TRANSACTION")
    END AS "TRANSACTION NOTE",    
    CASE
        WHEN TRIM("MEMO") = 'nan' THEN NULL
        ELSE TRIM("MEMO")
    END AS "MEMO",   
    FH_FILENAME,
    FH_TIMESTAMP 
FROM {{ source('ren_comm', 'bmo') }}
