 {{  config(alias='brokeremail_vc', database='normalize', schema='insurance')  }} 

SELECT
    AGENTCODE,
    TYPE,
    EMAILADDRESS,
    CASLAPPROVED
FROM {{ ref ('brokeremail_vc_clean_insurance')  }}