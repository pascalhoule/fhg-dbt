{{  config(alias='__comm_wgts', database='normalize', schema='insurance', tags=["policy_fh"], materialized="table")  }} 

    WITH FIN_TOTAL_PAID AS (
        SELECT
            A.POLICYCODE,
            A.OWNERCODE,
            A.SPLITSHARE / 100 AS AGENT_SPLIT,
            SUM(A.PAID) AS PAID
        FROM
            {{ ref ('accruals_clean_insurance') }} A
            LEFT JOIN {{ ref ('cheque_clean_insurance') }} C ON A.CHQCODE = C.CHQCODE
        WHERE
            TRXTYPE = 'FYC'
            AND C.POSTDATE::DATE >= '2018-01-01'
        GROUP BY
            1, 2, 3
        HAVING
            SUM(A.PAID) > 0
    ),
    FIN_AGT_SPLITS AS (
        SELECT
            A.POLICYCODE,
            A.OWNERCODE,
            A.SPLITSHARE / 100 AS AGENT_SPLIT,
            SUM(A.PAID) AS PAID
        FROM
            {{ ref ('accruals_clean_insurance') }} A
            LEFT JOIN {{ ref ('cheque_clean_insurance') }} C ON A.CHQCODE = C.CHQCODE
        WHERE
            TRXTYPE = 'FYC'
            AND C.POSTDATE::DATE >= '2018-01-01'
        GROUP BY
            1, 2, 3
        HAVING
            SUM(A.PAID) >= 0
    ),
    FIN_AGT_SPLITS_NOT_ONE AS (
        SELECT
            F.POLICYCODE,
            SUM(AGENT_SPLIT) AS TOT_SPLIT
        FROM
            FIN_AGT_SPLITS F
        GROUP BY
            1
        HAVING
            TOT_SPLIT > 1
    ),
    FIN_AGT_SPLITS_CORRECTED_RATIO AS (
        SELECT
            F.POLICYCODE,
            OWNERCODE,
            PAID,
            RATIO_TO_REPORT(PAID) OVER (PARTITION BY F.POLICYCODE) AS PD_AGT_SPLIT1,
            RATIO_TO_REPORT(AGENT_SPLIT) OVER (PARTITION BY F.POLICYCODE) AS PD_AGT_SPLIT2,
            coalesce(PD_AGT_SPLIT1, PD_AGT_SPLIT2) AS PD_AGT_SPLIT
        FROM
            FIN_AGT_SPLITS F
        WHERE
            F.POLICYCODE IN (
                SELECT
                    DISTINCT POLICYCODE
                FROM
                    FIN_AGT_SPLITS_NOT_ONE
            )
        ORDER BY
            F.POLICYCODE
    ),
    CORRECTED_AGT_SPLITS_BASE AS (
        SELECT
            P.POLICYCODE,
            P.OWNERCODE,
            COALESCE(S_CORR.PD_AGT_SPLIT, P.AGENT_SPLIT) AS CORR_PD_AGT_SPLIT,
            P.PAID AS PAID
        FROM
            FIN_TOTAL_PAID P
            LEFT JOIN FIN_AGT_SPLITS_CORRECTED_RATIO S_CORR ON P.POLICYCODE = S_CORR.POLICYCODE
            AND P.OWNERCODE = S_CORR.OWNERCODE
        GROUP BY
            1, 2, 3, 4
    ),
    CORRECTED_AGT_SPLITS_1 AS (
        SELECT
            POLICYCODE,
            OWNERCODE,
            SUM(CORR_PD_AGT_SPLIT) AS CORR_PD_AGT_SPLIT,
            SUM(PAID) AS PAID
        FROM
            CORRECTED_AGT_SPLITS_BASE
        GROUP BY
            1, 2
    ),
    SPLITS_GT_ONE AS (
        SELECT
            POLICYCODE,
            SUM(CORR_PD_AGT_SPLIT)
        FROM
            CORRECTED_AGT_SPLITS_1
        GROUP BY
            POLICYCODE
        HAVING
            SUM(CORR_PD_AGT_SPLIT) > 1
    ),
    SPLITS_GT_ONE_MAX_POSTDATE AS (
        SELECT
            A.POLICYCODE,
            MAX(c.POSTDATE) AS MAX_DATE,
        FROM
            {{ ref ('accruals_clean_insurance') }} A
            LEFT JOIN {{ ref ('cheque_clean_insurance') }} C ON A.CHQCODE = C.CHQCODE
        WHERE
            TRXTYPE = 'FYC'
            AND C.POSTDATE::DATE >= '2018-01-01'
            AND A.POLICYCODE IN (
                SELECT
                    DISTINCT POLICYCODE
                FROM
                    SPLITS_GT_ONE
            )
        GROUP BY
            1
    ),
    SPLITS_GT_ONE_SOME_CORRECTED AS (
        SELECT
            A.POLICYCODE,
            A.OWNERCODE,
            c.POSTDATE,
            M.MAX_DATE,
            A.SPLITSHARE / 100 AS AGENT_SPLIT,
            SUM(A.PAID) AS PAID
        FROM
            {{ ref ('accruals_clean_insurance') }} A
            LEFT JOIN {{ ref ('cheque_clean_insurance') }} C ON A.CHQCODE = C.CHQCODE
            LEFT JOIN SPLITS_GT_ONE_MAX_POSTDATE M on M.POLICYCODE = A.POLICYCODE
            AND M.MAX_DATE = C.POSTDATE
        WHERE
            TRXTYPE = 'FYC'
            AND C.POSTDATE::DATE >= '2018-01-01'
            AND A.POLICYCODE IN (
                SELECT
                    DISTINCT POLICYCODE
                FROM
                    SPLITS_GT_ONE
            )
            AND M.MAX_DATE is not null
        GROUP BY
            1, 2, 3, 4, 5
    ),
    FIN_AGT_SPLITS_NOT_ONE_PART2 AS (
        SELECT
            F.POLICYCODE,
            SUM(AGENT_SPLIT) AS TOT_SPLIT
        FROM
            SPLITS_GT_ONE_SOME_CORRECTED F
        GROUP BY
            1
        HAVING
            TOT_SPLIT > 1
    ),
    FIN_AGT_SPLITS_CORRECTED_RATIO_PART2 AS (
        SELECT
            F.POLICYCODE,
            OWNERCODE,
            PAID,
            RATIO_TO_REPORT(PAID) OVER (PARTITION BY F.POLICYCODE) AS PD_AGT_SPLIT1,
            RATIO_TO_REPORT(AGENT_SPLIT) OVER (PARTITION BY F.POLICYCODE) AS PD_AGT_SPLIT2,
            coalesce(PD_AGT_SPLIT1, PD_AGT_SPLIT2) AS PD_AGT_SPLIT
        FROM
            SPLITS_GT_ONE_SOME_CORRECTED F
        WHERE
            F.POLICYCODE IN (
                SELECT
                    DISTINCT POLICYCODE
                FROM
                    FIN_AGT_SPLITS_NOT_ONE_PART2
            )
        ORDER BY
            F.POLICYCODE
    ),
    CORRECTED_AGT_SPLITS_BASE_PART2 AS (
        SELECT
            P.POLICYCODE,
            P.OWNERCODE,
            COALESCE(S_CORR.PD_AGT_SPLIT, P.AGENT_SPLIT) AS CORR_PD_AGT_SPLIT,
            P.PAID AS PAID
        FROM
            SPLITS_GT_ONE_SOME_CORRECTED P
            LEFT JOIN FIN_AGT_SPLITS_CORRECTED_RATIO_PART2 S_CORR ON P.POLICYCODE = S_CORR.POLICYCODE
            AND P.OWNERCODE = S_CORR.OWNERCODE
            AND p.PAID = S_CORR.PAID
        GROUP BY
            1, 2, 3, 4
    ),
    CORRECTED_AGT_SPLITS_2 AS (
        SELECT
            POLICYCODE,
            OWNERCODE,
            SUM(CORR_PD_AGT_SPLIT) AS CORR_PD_AGT_SPLIT,
            SUM(PAID) AS PAID
        FROM
            CORRECTED_AGT_SPLITS_BASE_PART2
        GROUP BY
            1, 2
    )
    SELECT
        POLICYCODE,
        OWNERCODE,
        CORR_PD_AGT_SPLIT,
        PAID
    from
        CORRECTED_AGT_SPLITS_2
    UNION
    SELECT
        POLICYCODE,
        OWNERCODE,
        CORR_PD_AGT_SPLIT,
        PAID
    FROM
        CORRECTED_AGT_SPLITS_1
    WHERE
        POLICYCODE NOT IN (
            SELECT
                DISTINCT POLICYCODE
            FROM
                CORRECTED_AGT_SPLITS_2
        )
