{{ config( 
    alias='canada_life_adjusted_for_contract_dates', 
    database='agt_comm', 
    schema='contest', 
    materialized = "view" ) }}


 SELECT
    CL.YR,
    CL.MTH,
    CL.CL_UID,
    CL.FH_UID,
    INITCAP(CL.ADVISOR_NAME) AS ADVISOR_NAME,
    REGION,
    MARKET,
    LOCATION,
    INITCAP(CL.SALES_DIRECTOR) AS SALES_DIRECTOR,
    CASE
        WHEN ADJ.ADVISOR_NAME IS NULL THEN SEGFUNDS_NET_SALES
        ELSE 0
    END AS SEGFUNDS_NET_SALES,
    SEGFUNDS_TOTAL_SALES,
    SEGFUNDS_TOTAL_REDEMPTIONS,
    CASE
        WHEN ADJ.ADVISOR_NAME IS NULL THEN SEGFUNDS_AUM
        ELSE 0
    END AS SEGFUNDS_AUM,
    CASE
        WHEN ADJ.ADVISOR_NAME IS NULL THEN MUTUAL_FUNDS_NET_SALES
        ELSE 0
    END AS MUTUAL_FUNDS_NET_SALES,
    MUTUAL_FUNDS_TOTAL_SALES,
    MUTUAL_FUNDS_TOTAL_REDEMPTIONS,
    CASE
        WHEN ADJ.ADVISOR_NAME IS NULL THEN MUTUAL_FUNDS_AUA
        ELSE 0
    END AS MUTUAL_FUNDS_AUA,
    CASE
        WHEN ADJ.ADVISOR_NAME IS NULL THEN FYC_INSURANCE
        ELSE 0
    END AS FYC_INSURANCE
FROM
    {{ source('contest', 'canada_life') }} CL
    LEFT JOIN {{ source('contest', 'contract_dt_adjustment') }} ADJ ON CL.MTH = ADJ.MTH
    AND CL.YR = ADJ.YR
    AND CL.FH_UID = 'FH' || ADJ.FH_UID   