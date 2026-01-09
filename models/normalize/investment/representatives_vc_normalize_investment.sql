{{ config(alias='representatives_vc', database='normalize', schema='investment') }} 


SELECT
    REPRESENTIATIVECODE AS REPRESENTATIVECODE,
    INSAGENTCODE,
    REPID,
    FIRST_NAME,
    LAST_NAME,
    DOB,
    BRANCH_CODE,
    SIN,
    CODE,
    REPSTATUS
FROM {{ ref ('representatives_vc_clean_investment') }}
