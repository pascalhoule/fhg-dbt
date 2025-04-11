{{ config( 
    alias='f_mf_sales_credits', 
    database='agt_comm', 
    schema='contest', 
    materialized = "view" ) }}

WITH TEMP AS (
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
            {{ source('contest', 'quadrus_sales') }} agt_comm.prod_contest.quadrus_sales AS Q
            LEFT JOIN {{ ref('a_dim_agt_comm_contest') }} agt_comm.prod_contest.a_dim AS DIM ON 
            TRIM(Q.FIRST_NAME) = UPPER(SPLIT_PART(DIM.FIRSTNAME, ' ', 1))
            AND TRIM(Q.LAST_NAME) = UPPER(DIM.LASTNAME)
            LEFT JOIN {{ ref('a_dim_inactive_agt_comm_contest') }} ON TRIM(Q.FIRST_NAME) = UPPER(SPLIT_PART(DIM_INACTIVE.FIRSTNAME, ' ', 1))
            AND TRIM(Q.LAST_NAME) = UPPER(DIM_INACTIVE.LASTNAME)
        WHERE
            Q.INCL_IN_REPORT = TRUE
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