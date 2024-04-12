{{  config(alias='broker_vc', database='report', schema='insurance')  }} 
 

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