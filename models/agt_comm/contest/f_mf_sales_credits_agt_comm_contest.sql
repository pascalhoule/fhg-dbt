{{ config( 
    alias='f_mf_sales_credits', 
    database='agt_comm', 
    schema='contest', 
    materialized = "view" ) }}

WITH DIM AS (
        SELECT
            *
        FROM
            {{ ref('a_dim_agt_comm_contest') }}
        WHERE
            AGENTSTATUS = 'Active'
    ),
    TEMP AS (
        SELECT
            COALESCE(
                DIM.USERDEFINED2,
                DIM_INACTIVE.USERDEFINED2,
                DIM.BROKERID
            ) AS UD_2,
            COALESCE(DIM.FIRSTNAME, DIM_INACTIVE.FIRSTNAME) AS FIRSTNAME,
            COALESCE(DIM.LASTNAME, DIM_INACTIVE.LASTNAME) AS LASTNAME,
            SUM(Q.SALES) AS MF_SALES,
            SUM(Q.SALES) * 0.03 AS MF_SALES_CREDITS
        FROM
            {{ source('contest', 'quadrus_sales') }} AS Q
            JOIN {{ source('contest', 'date_ranges') }} D ON Q.YR = D.QUADRUS_YR and Q.MTH = D.QUADRUS_MTH
    
            LEFT JOIN DIM ON TRIM(Q.FIRST_NAME) = UPPER(SPLIT_PART(DIM.FIRSTNAME, ' ', 1))
            AND TRIM(Q.LAST_NAME) = UPPER(DIM.LASTNAME)
            LEFT JOIN {{ ref('a_dim_inactive_agt_comm_contest') }} DIM_INACTIVE ON TRIM(Q.FIRST_NAME) = UPPER(SPLIT_PART(DIM_INACTIVE.FIRSTNAME, ' ', 1))
            AND TRIM(Q.LAST_NAME) = UPPER(DIM_INACTIVE.LASTNAME)
        WHERE
            D.INCL_IN_RPT = TRUE
            AND Q.SALES IS NOT NULL
            AND UD_2 IS NOT NULL
            AND CONTAINS(UD_2, '/') = FALSE
            AND (
                DIM.IS_PRIMARY = TRUE
                OR DIM_INACTIVE.IS_PRIMARY = TRUE
            )
        GROUP BY
            1, 2, 3
    )
    SELECT
        *
    FROM
        TEMP
    GROUP BY
        ALL
    UNION
        --This is for exception management.
    SELECT
        U_CODE AS UD_2,
        FIRST_NAME AS FIRSTNAME,
        LAST_NAME AS LASTNAME,
        SALES AS MF_SALES,
        0.03 * SALES AS MF_SALES_CREDITS,
    FROM
        {{ source('contest', 'quadrus_sales') }} Q JOIN {{ source('contest', 'date_ranges') }} D ON Q.YR = D.QUADRUS_YR and Q.MTH = D.QUADRUS_MTH
    WHERE
        U_CODE in ('U0000101350', 'U0000140583')
        and D.INCL_IN_RPT = TRUE