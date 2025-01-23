{{ config(
    alias='GA_carriercontract', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH Temp AS (
    SELECT Cc.*
    FROM
        {{ ref('carriercontract_V_salesforce_exports') }} AS Cc
    WHERE
        EXISTS (
            SELECT 1
            FROM {{ ref('broker_V_salesforce_exports') }} AS Bt
            WHERE Bt.Agentcode = Cc.Agent_code
        )
    ORDER BY Cc.Broker_contract_code

)

SELECT * FROM Temp
