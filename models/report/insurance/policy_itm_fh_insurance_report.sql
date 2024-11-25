{{			
    config (			
        materialized="view",			
        alias='policy_itm_fh', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT
    POL.*,
    ITM.ITM_END_DATE AS FH_ITM_END_DATE,
    ITM.ITM AS FH_ITM,
    ITM.DAYS_IN_STATUS AS FH_DAYS_IN_STATUS
FROM
    {{ ref('policy_fh_report_insurance') }} POL
    LEFT JOIN {{ ref('itm_calcs_FH_report_insurance') }} ITM ON POL.POLICYCODE = ITM.POLICYCODE
    AND POL.FH_SERVICINGAGTCODE = ITM.FH_SERVICINGAGTCODE
    AND POL.POLICYNUMBER = ITM.POLICYNUMBER
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
    31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54