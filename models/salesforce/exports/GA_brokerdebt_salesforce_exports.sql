{{ config(
    alias='GA_brokerdebt', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

SELECT bdbt.*
FROM
    {{ ref('brokerdebt_salesforce_exports') }} AS bdbt
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('broker_salesforce_exports') }} AS bt
    WHERE bt.agentcode = bdbt.agentcode
)
