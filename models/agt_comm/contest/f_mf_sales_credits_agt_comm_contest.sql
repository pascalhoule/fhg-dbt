{{ config( 
    alias='f_mf_sales_credits', 
    database='agt_comm', 
    schema='contest', 
    materialized = "view" ) }}

SELECT
    COALESCE(DIM.USERDEFINED2, DIM.BROKERID) AS UD_2,
    DIM.FIRSTNAME,
    DIM.LASTNAME,
    SUM(Q.SALES) AS MF_SALES,
    SUM(Q.SALES) * 0.03 AS MF_SALES_CREDITS
FROM
    {{ source('contest', 'quadrus_sales') }} AS Q
LEFT JOIN {{ ref('a_dim_agt_comm_contest') }} AS DIM
    ON
        TRIM(Q.FIRST_NAME) = UPPER(SPLIT_PART(DIM.FIRSTNAME,' ', 1)) 
        AND TRIM(Q.LAST_NAME) = UPPER(DIM.LASTNAME)
WHERE
    Q.INCL_IN_REPORT = TRUE
    AND Q.SALES IS NOT NULL
    AND UD_2 IS NOT NULL
    AND CONTAINS(UD_2, '/') = FALSE
    AND IS_PRIMARY = TRUE
GROUP BY
    1, 2, 3