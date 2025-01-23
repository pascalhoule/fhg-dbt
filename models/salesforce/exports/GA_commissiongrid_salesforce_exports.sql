{{ config(
    alias='GA_commissiongrid', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}


WITH TEMP AS (
   SELECT  
    cg.UID,
    cg.LIFE_OVERRIDE,
    cg.SEG_OVERRIDE,
    cg.TEMPLATENAME as TEMPLATE_NAME,
    cg.EFFECTIVEDATE as EFFECTIVE_DATE,
    cg.ENDDATE as END_DATE,
    cg.TEMPLATETYPE as TEMPLATE_TYPE,
    cg.AGENT_CODE
FROM
    {{ ref('commissiongrid_V_salesforce_exports') }} cg
WHERE EXISTS (
    SELECT 1 
    FROM {{ ref('broker_V_salesforce_exports') }} bt
    WHERE bt.AGENTCODE = cg.AGENT_CODE
)
)

SELECT * FROM TEMP
