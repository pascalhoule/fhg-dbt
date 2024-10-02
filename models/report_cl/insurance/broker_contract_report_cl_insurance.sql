{{  config(alias='broker_contract', database='report_cl', schema='insurance')  }}

SELECT
    BROKER.PARENTNODEID,
    BROKER.AGENTCODE,
    BROKER.BROKERID,
    CONTRACT.CONTRACTNUMBER,
    CONTRACT.CREATEDDATE,
    CONTRACT.LICENSEDATE::date AS LICENSEDATE,
    ICNAME_ENG,
    ICNAME_FR
FROM
    {{ ref ('broker_vc_report_insurance') }}  BROKER
    LEFT JOIN {{ ref ('brokercontract_vc_report_insurance') }}  CONTRACT on CONTRACT.AGENTCODE = BROKER.AGENTCODE
    LEFT JOIN {{ ref ('ic_vc_report_insurance') }}  IC on IC.ICCODE = CONTRACT.ICCODE
    --WHERE BROKER.AGENTSTATUS in ('Active', 'Pending') AND CONTRACT.BROKERCONTRACTSTATUSCODE in (0,1,11) --active, pend-brk, pend-carr