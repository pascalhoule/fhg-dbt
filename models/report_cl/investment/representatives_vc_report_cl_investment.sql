{{ config(alias='representatives_vc', database='report_cl', schema='investment') }} 


SELECT
    REPRESENTATIVECODE,
    INSAGENTCODE,
    REPID,
    FIRST_NAME,
    LAST_NAME,
    DOB,
    BRANCH_CODE,
    SIN,
    CODE,
    REPSTATUS
FROM {{ ref ('representatives_vc_analyze_investment') }}
