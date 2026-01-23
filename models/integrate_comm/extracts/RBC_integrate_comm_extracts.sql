{{ config(
    alias='rbc', 
    database='integrate_comm', 
    schema='extracts', 
    materialized='view',
    grants={'ownership': ['COMMISSION']},
) }} 

SELECT
    'RBC' as "COMPANY",
    TOP AS "DISTRIBUTOR CODE",
    "ADVISOR'S CODE" AS "AGENT SELLING CODE",
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
    DATE("STATEMENT DATE") AS "GENERATION DATE",
    "COMM. TYPE" AS "COMMISSION TYPE",
    CASE
        WHEN TRIM("MODE") = 'nan' THEN NULL
        ELSE TRIM("MODE")
    END AS "MODE",  
      CASE
        WHEN TRIM("PRODUCT TYPE") = 'nan' THEN NULL
        ELSE TRIM("PRODUCT TYPE")
    END AS "PRODUCT",  
    CASE
        WHEN TRIM(CAST("PLAN CODE SHORT DESC." AS VARCHAR)) = 'nan' THEN NULL
        ELSE TRIM(CAST("PLAN CODE SHORT DESC." AS VARCHAR))
    END AS "BASIC PLAN CODE",
    RATE AS "COMMISSION RATE",
    "COMPENSATION BASE AMOUNT" AS "STATEMENT BASIS AMOUNT",
    "AMOUNT",
    SHARE as "SHARE %",
    CASE
        WHEN TRIM("BUS. REPORTING NAME") = 'nan' THEN NULL
        ELSE TRIM("BUS. REPORTING NAME")
    END AS "BUSINESS SURNAME",    
    CASE
    	WHEN LEFT("COMM. TYPE", 3) IN ('FYC', 'FOV') THEN 'First Year' 
	    WHEN LEFT("COMM. TYPE", 3) IN ('REN', 'ROV') THEN 'Renewal'
	    WHEN LEFT("COMM. TYPE", 3) IN ('DEB') THEN 'Debt'
    	WHEN LEFT("COMM. TYPE", 3) IN ('MSC') THEN 'Other'
        ELSE 'N/A'
    END AS "COMMISSION TAG",
    FH_FILENAME, 
    FH_TIMESTAMP
FROM
    {{ source('ren_comm', 'rbc') }}