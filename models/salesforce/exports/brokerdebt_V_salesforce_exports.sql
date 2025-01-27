{{ config(
    alias='brokerdebt_V', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH CTE_CARRIERDEBTGROUPBY AS (
    SELECT
        AGENTCODE,
        SUM(CAST(BALANCE AS FLOAT)) AS CARRIER_DEBT,
        ROUND(SUM(CASE
            WHEN
                DATEDIFF(
                    'd', CAST(ACTIVATED_DATE AS DATE), CURRENT_DATE
                ) BETWEEN 30 AND 60
                THEN CAST(BALANCE AS FLOAT)
            ELSE 0
        END), 2) AS TCD30,
        ROUND(SUM(CASE
            WHEN
                DATEDIFF('d', CAST(ACTIVATED_DATE AS DATE), CURRENT_DATE) >= 30
                THEN CAST(BALANCE AS FLOAT)
            ELSE 0
        END), 2) AS TCD30_60
    FROM {{ ref('carrierdebt_V_salesforce_exports') }}
    GROUP BY AGENTCODE
)

SELECT
    BLS.AGENTCODE,
    0 AS MGA_DEBT,
    BLS.PAYABLEBALANCE AS LEDGER_BALANCE,
    BLS.ORIGINALDEBTDATE AS ORIGINAL_DEBT_DATE,
    '' AS REPAYMENT_PLAN,
    '' AS REPAYMENT_PLAN_CONTACT,
    '' AS REPAYMENT_SENT_TO_COLLECTIONS,
    '' AS SENT_COLLECTIONS_DATE,
    '' AS REPAYMENT_WRITTEN_OFF,
    '' AS WRITTEN_OFF_DATE,
    BLS.AGENTCODE AS EXTERNAL_AGENT_CODE_ID,
    COALESCE(CTE.TCD30, 0) AS TCD30,
    COALESCE(CTE.TCD30_60, 0) AS TCD30_60
FROM {{ ref('brokerledgersummary_vc_salesforce_insurance') }} AS BLS
LEFT JOIN CTE_CARRIERDEBTGROUPBY AS CTE
    ON BLS.AGENTCODE = CTE.AGENTCODE
ORDER BY BLS.AGENTCODE
