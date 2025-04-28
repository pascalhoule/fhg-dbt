{{ config( alias='c_segfund_credits', database='agt_comm', schema='contest', materialized = "view" ) }} 

WITH DATES AS (
            SELECT
                SEG_FUNDS_STARTDATE,
                SEG_FUNDS_ENDDATE
            FROM
                {{ source('contest', 'date_ranges') }}
            WHERE
                INCL_IN_RPT = TRUE
                ),
     AMT AS (            
            SELECT
            BA.USERDEFINED2 AS UD2,
            BA.AGENTCODE,
            SUM(T.AMOUNT * TT.DBSIGN) AS SEG_SALES_AMOUNT,
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
    1, 2),


 CREDITS AS
    (SELECT UD2, SUM(SEG_SALES_AMOUNT) AS SALES,
    CASE WHEN SUM(SEG_SALES_AMOUNT) < 0 THEN 0 ELSE 0.03*SUM(SEG_SALES_AMOUNT) END AS SEG_SALES_CREDIT
    FROM AMT
    GROUP BY ALL
    )

 SELECT 
    A.UD2, 
    A.AGENTCODE, 
    C.SALES AS SEG_SALES_AMOUNT, 
    C.SEG_SALES_CREDIT
 FROM CREDITS C JOIN AMT A on C.UD2 = A.UD2

  