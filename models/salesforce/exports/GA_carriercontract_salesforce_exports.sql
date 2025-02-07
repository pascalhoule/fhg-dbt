{{ config(
    alias='GA_carriercontract', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH Temp AS (
    SELECT Cc.*
    FROM
        {{ ref('carriercontract_salesforce_exports') }} AS Cc
    WHERE
        EXISTS (
            SELECT 1
            FROM {{ ref('broker_salesforce_exports') }} AS Bt
            WHERE Bt.Agentcode = Cc.Agentcode
        )
    ORDER BY Cc.BROKERCONTRACTCODE

)

SELECT * FROM Temp
