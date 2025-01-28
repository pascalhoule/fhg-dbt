{{ config(
    alias='GA_carrierdebt', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}


WITH TEMP AS (
    SELECT CD.*
    FROM
        {{ ref('carrierdebt_salesforce_exports') }} AS CD
    WHERE EXISTS (
        SELECT 1
        FROM {{ ref('broker_salesforce_exports') }} AS BT
        WHERE BT.AGENTCODE = CD.AGENTCODE
    )
)

SELECT * FROM TEMP
