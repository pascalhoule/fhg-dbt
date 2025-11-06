{{ config(
    alias='contracts', 
    database='applications', 
    schema='carrier_emails',
    materialized="view",
    grants = {'ownership': ['FH_READER']}) 
}}

SELECT 
    a.MGAID,
    ICAgent.AgentCode,
    a.FirstName,
    a.LastName,
    ICAgent.LicenseCode,
    ICAgent.ICCode,
    IC.ICID,
    IC.ICName,
    ICAgent.Contract_Type,
    c.Description AS Contract_Type_Description,
    CASE
        WHEN c.Description = 'Personal' THEN 'Personnel'
        WHEN c.Description = 'Rep-Entitled' THEN 'Conseiller en délégation'
        ELSE 'Corporation'
    END AS Contract_Type_FR,
    ICAgent.Status AS Contract_Status,
    c2.Description AS Contract_Status_Description,
    CASE
        WHEN c2.Description = 'Active' THEN 'Actif'
        WHEN c2.Description = 'Pend-Carr' THEN 'En Attente - Assureur'
        ELSE 'Inactif'
    END AS Contract_Status_FR
FROM {{ ref('icagent_clean_insurance') }} ICAGENT 
LEFT JOIN {{ ref('ic_clean_insurance') }} IC ON IC.ICCODE = ICAGENT.ICCODE
LEFT JOIN {{  ref('constants_clean_insurance')}} C ON C.VALUE = ICAGENT.CONTRACT_TYPE
LEFT JOIN {{ ref('constants_clean_insurance') }} C2 ON C2.VALUE = ICAGENT.STATUS
LEFT JOIN {{ ref('agent_clean_insurance') }} A ON A.AGENTCODE = ICAGENT.AGENTCODE
WHERE
    IC.ICID <> 'FundServ'
    AND C.TYPE = 'Contract_Type'
    AND C2.TYPE = 'CarrierBrokerStatus'