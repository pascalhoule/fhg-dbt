{{ config(
    alias='GA_carriercontractdeleted', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

WITH Temp AS (
    SELECT
        Ica.Icagentcode AS Brokercontractcode,
        Ic.Icname,
        C1.Description AS Contract_type,
        Ica.Licensecode AS Contractnumber,
        C2.Description AS Contract_status,
        Ica.Agentcode AS Agent_code
    FROM {{ ref('icagent_salesforce_insurance') }} AS Ica
    LEFT JOIN
        {{ ref('ic_salesforce_insurance') }} AS Ic
        ON Ica.Iccode = Ic.Iccode
    LEFT JOIN
        {{ ref('constants_salesforce_insurance') }} AS C1
        ON Ica.Contract_type = C1.Value
    LEFT JOIN
        {{ ref('constants_salesforce_insurance') }} AS C2
        ON Ica.Status = C2.Value
    WHERE
        C1.Type = 'Contract_Type'
        AND C2.Type = 'CarrierBrokerStatus'
        AND TRIM(C2.Description) = 'Deleted'
    ORDER BY Ic.Icname
)

SELECT * FROM Temp
