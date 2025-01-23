{{ config(
    alias='GA_brokercos', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH Temp AS (
    SELECT Bs.*
    FROM
        {{ ref('brokercos_V_salesforce_exports') }} AS Bs
    WHERE EXISTS (
        SELECT 1
        FROM {{ ref('GA_broker_salesforce_exports') }} AS Bt
        WHERE Bt.Agentcode = Bs.Agentcode
    )
)

SELECT * FROM Temp
