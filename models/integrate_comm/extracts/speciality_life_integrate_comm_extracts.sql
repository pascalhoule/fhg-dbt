{{ config(alias='speciality_life', 
    database='integrate_comm', 
    schema='extracts', 
    materialization = "view",
    grants = {'ownership': ['COMMISSION']},)  
}} 

SELECT
    DATE,
    TRIM("SOURCE INFO") AS SOURCE_INFO, 
    TRIM("WRITING ADVISOR") AS WRITING_ADVISOR, 
    TRIM("ADVISOR CODE") AS ADVISOR_CODE, 
    TRIM("POLICY NUMBER") AS POLICY_NUMBER, 
    TRIM(TRANSACTION) AS TRANSACTION, 
    TRIM("TRANSACTION TYPE (NEW)") AS TRANSACTION_TYPE_NEW, 
    TRIM(TYPE) AS TYPE, 
    RATE, 
    BASIS, 
    SHARE, 
    AMOUNT, 
    BALANCE, 
    FH_FILENAME, 
    FH_TIMESTAMP
FROM {{ source('ren_comm', 'sli') }}
