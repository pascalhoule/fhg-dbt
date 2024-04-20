{{ config(
    database = "normalize"
    alias = "COMM_WGT_ONE_NORMALIZE"
    severity = "warn",
    store_failures = true
)}}

SELECT
    POLICYCODE,
    ROUND(SUM(CORR_PD_AGT_SPLIT), 3) AS TOT_WGT
FROM
    {{ ref('__COMM_WGTS_insurance') }}
GROUP BY
    1
HAVING
    TOT_WGT <> 1