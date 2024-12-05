{{  config(alias='broker_vc', database='report_cl', schema='insurance', grants = {'ownership': ['BI_DEV']},)  }} 
 

SELECT
    PARENTNODEID,
    AGENTCODE,
    FIRSTNAME,
    MIDDLENAME,
    LASTNAME,
    FULLAGENTNAME,
    AGENTNAME,
    COMPANYNAME,
    MGACODE,
    AGACODE,
    DATEOFBIRTH,
    PROVINCE,
    COMPANYPROVINCE,
    AGENTSTATUS,
    AGENTTYPE,
    LANGUAGEPREFERENCE,
    SERVICELEVEL,
    CREATEDDATE,
    BROKERID,
    LASTMODIFIEDDATE
  
from {{ ref ('broker_vc_analyze_insurance')  }}