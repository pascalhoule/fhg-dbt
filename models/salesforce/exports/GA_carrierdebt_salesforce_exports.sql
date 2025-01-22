{{ config(
    alias='GA_carrierdebt', 
    database='salesforce', 
    schema='exports',
   materialized="view") }}


WITH TEMP AS (
    SELECT CD.*
    FROM
        {{ ref('carrierdebt_V_salesforce_exports') }} AS CD
    WHERE EXISTS (
        SELECT 1
        FROM {{ ref('broker_V_salesforce_exports') }} AS BT
        WHERE BT.AGENTCODE = CD.AGENTCODE
    )
)

SELECT * FROM TEMP
