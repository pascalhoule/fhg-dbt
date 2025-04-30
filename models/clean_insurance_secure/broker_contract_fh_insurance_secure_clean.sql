{{  config(alias='ins_broker_contract_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT
    BROKER.PARENTNODEID,
    BROKER.AGENTCODE,
    BROKER.BROKERID,
    CONTRACT.CONTRACTNUMBER,
    CONTRACT.CREATEDDATE,
    CONTRACT.LICENSEDATE::date AS LICENSEDATE,
    SPLIT_PART(ICNAME, ' / ', 1) AS ICNAME_ENG,
    CASE
        WHEN SPLIT_PART(ICNAME, ' / ', 2) = '' THEN SPLIT_PART(ICNAME, ' / ', 1)
        ELSE SPLIT_PART(ICNAME, ' / ', 2)
    END AS ICNAME_FR
FROM
    {{ ref('broker_vc_clean_insurance') }}  BROKER
    LEFT JOIN {{ ref('brokercontract_vc_clean_insurance') }} CONTRACT on CONTRACT.AGENTCODE = BROKER.AGENTCODE
    LEFT JOIN {{ ref('ic_vc_clean_insurance') }}  IC on IC.ICCODE = CONTRACT.ICCODE