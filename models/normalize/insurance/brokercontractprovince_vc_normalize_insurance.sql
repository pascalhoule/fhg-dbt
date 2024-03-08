{{  config(alias='brokercontractprovince_vc', database='normalize', schema='insurance')  }} 


SELECT
    LICENSECODE,
    LICENSENUMBER,
    STARTDATE,
    ENDDATE,
    AGENTCODE,
    TYPE,
    CATEGORY,
    PROVINCE,
    LASTMODIFIEDDATE  
FROM
  {{ ref ('brokercontractprovince_vc_clean_insurance') }}