{{ config( alias='c_segfund_credits', database='agt_comm', schema='contest', materialized = "view" ) }} 

WITH DATES AS (
            SELECT
                SEG_FUNDS_STARTDATE,
                SEG_FUNDS_ENDDATE
            FROM
                {{ source('contest', 'date_ranges') }}
            WHERE
                INCL_IN_RPT = TRUE
                )
SELECT
    BA.USERDEFINED2 AS UD2,
    BA.AGENTCODE,
    SUM(T.AMOUNT * TT.DBSIGN) AS SEG_SALES_AMOUNT,
    CASE
        WHEN SUM(T.AMOUNT * TT.DBSIGN) > 0 THEN SUM(T.AMOUNT * TT.DBSIGN) * 0.03
        ELSE 0
    END AS SEG_SALES_CREDIT
FROM
    {{ ref('__base_transactions_for_contest_agt_comm_contest') }} AS T
JOIN
    {{ ref('a_dim_agt_comm_contest') }} AS BA
    ON T.REPID = BA.AGENTCODE
INNER JOIN
    {{ ref('__base_segfund_transtypes_agt_comm_contest') }} AS TT
    ON T.TRANSACTIONTYPECODE = TT.TRANSACTIONTYPECODE
WHERE
    TRADEDATE >= (
                SELECT
                    SEG_FUNDS_STARTDATE
                FROM
                    DATES
            )
            AND TRADEDATE <= (
                SELECT
                    SEG_FUNDS_ENDDATE
                FROM
                    DATES
    )
GROUP BY
    1, 2
