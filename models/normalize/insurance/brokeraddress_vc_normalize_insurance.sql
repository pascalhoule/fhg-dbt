 {{  config(alias='brokeraddress_vc', database='normalize', schema='insurance')  }} 


SELECT
    AGENTCODE,
    TYPE,
    ADDRESS,
    CITY,
    PROVINCE,
    POSTAL_CODE,
    COUNTRY
FROM
  {{ ref ('brokeraddress_vc_clean_insurance') }}