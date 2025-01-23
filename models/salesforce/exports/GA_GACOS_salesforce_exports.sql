{{ config(
    alias='GA_GACOS', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH Temp AS (
    SELECT Bs.*
    FROM
        {{ ref('GACOS_V_salesforce_exports') }} AS Bs
    WHERE EXISTS (
        SELECT 1
        FROM {{ ref('GA_accounts_global_salesforce_exports') }} AS Ag
        WHERE Ag.Uid = Bs.Ga_uid
    )
)

SELECT * FROM Temp
