{{ config(
    alias='commissiongrid_V', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']})  }}

WITH CTE_SEGS AS (
    SELECT DISTINCT 
        CONCAT('uid_', CG.OWNERCODE, '_', CG.GRIDCODE) AS UID,		  
        SUBSTR(CG.TEMPLATENAME, 0, 4) AS SEG_OVERRIDE,
        CG.TEMPLATENAME,
        CG.EFFECTIVEDATE,
        CG.ENDDATE,
        CG.TEMPLATETYPE,
        CG.BUSINESS,
        CG.OWNERCODE AS AGENT_CODE
    FROM {{ ref('commissiongrid_vc_salesforce_insurance') }} CG
    LEFT JOIN {{ ref('hierarchy_vc_salesforce_insurance') }} H 
        ON H.NODEID = CG.OWNERNODEID
    WHERE CG.STATUS = 'active' 
      AND CURRENT_DATE BETWEEN CG.EFFECTIVEDATE AND CG.ENDDATE
      AND CG.BUSINESS = 'Investment'
      AND CG.OWNERTYPE = 'Broker'
)
SELECT DISTINCT 
    CONCAT('uid_', CG.OWNERCODE, '_', CG.GRIDCODE) AS UID,
    SUBSTR(CG.TEMPLATENAME, 0, 4) AS LIFE_OVERRIDE,
    NVL(CTES.SEG_OVERRIDE, '000') AS SEG_OVERRIDE,
    CG.TEMPLATENAME,
    CG.EFFECTIVEDATE,
    CG.ENDDATE,
    CG.TEMPLATETYPE,
    CG.BUSINESS,
    CG.OWNERCODE AS AGENT_CODE
FROM {{ ref('commissiongrid_vc_salesforce_insurance') }} CG
LEFT JOIN {{ ref('hierarchy_vc_salesforce_insurance') }} H 
    ON H.NODEID = CG.OWNERNODEID
LEFT JOIN CTE_SEGS CTES 
    ON CTES.AGENT_CODE = CG.OWNERCODE
WHERE CG.STATUS = 'active' 
  AND CURRENT_DATE BETWEEN CG.EFFECTIVEDATE AND CG.ENDDATE
  AND CG.BUSINESS = 'Insurance'
  AND CG.OWNERTYPE = 'Broker'
ORDER BY CG.OWNERCODE