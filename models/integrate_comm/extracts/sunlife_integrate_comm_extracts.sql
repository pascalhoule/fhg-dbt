{{ config(alias='sunlife', 
    database='integrate_comm', 
    schema='extracts', 
    materialization = "view",
    grants = {'ownership': ['COMMISSION']},)  
}} 

SELECT
    TRIM(BRANCH) AS BRANCH,
    "FROM/TO ID" AS FROM_TO_ID,
    DATE("GENERATION DATE") AS GENERATION_DATE,
    "FIELD FORCE ID" AS FIELD_FORCE_ID,
    CASE 
        WHEN TRIM("BUSINESS FIRST NAME") = 'nan' THEN NULL
        ELSE TRIM("BUSINESS FIRST NAME")
    END AS BUSINESS_FIRST_NAME,
    TRIM("BUSINESS SURNAME") AS BUSINESS_SURNAME,
    CASE 
        WHEN TRIM("MULTI CORP") = 'nan' THEN NULL
        ELSE TRIM("MULTI CORP")
    END AS MULTI_CORP,
    CASE 
        WHEN TRIM("POLICY") = 'nan' THEN NULL
        ELSE TRIM("POLICY")
    END AS POLICY,
    CASE 
        WHEN TRIM("FIRST NAME") = 'nan' THEN NULL
        ELSE TRIM("FIRST NAME")
    END AS FIRST_NAME,
    CASE 
        WHEN TRIM("LAST NAME") = 'nan' THEN NULL
        ELSE TRIM("LAST NAME")
    END AS LAST_NAME,
    CASE 
        WHEN TRIM("COMM. SUBTYPE") = 'nan' THEN NULL
        ELSE TRIM("COMM. SUBTYPE")
    END AS COMM_SUBTYPE,
    "STMT BASIS AMOUNT" AS STMT_BASIS_AMOUNT,
    "COMMISSION RATE" AS COMMISSION_RATE,
    AMOUNT,
    SHARE,
    CASE 
        WHEN TRIM("BASIC PLAN CODE") = 'nan' THEN NULL
        ELSE TRIM("BASIC PLAN CODE")
    END AS BASIC_PLAN_CODE,
    CASE 
        WHEN TRIM("EIM DIVISION") = 'nan' THEN NULL
        ELSE TRIM("EIM DIVISION")
    END AS EIM_DIVISION,
    CASE 
        WHEN TRIM("EIM LOB") = 'nan' THEN NULL
        ELSE TRIM("EIM LOB")
    END AS EIM_LOB,
    CASE 
        WHEN TRIM("EIM PRODUCT") = 'nan' THEN NULL
        ELSE TRIM("EIM PRODUCT")
    END AS EIM_PRODUCT,
    FH_FILENAME, 
    TO_TIMESTAMP(FH_TIMESTAMP) AS FH_TIMESTAMP
FROM {{ source('ren_comm', 'sunlife') }}