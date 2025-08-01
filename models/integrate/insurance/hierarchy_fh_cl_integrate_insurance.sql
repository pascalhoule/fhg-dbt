{{			
    config (			
        materialized="view",			
        alias='hierarchy_fh_cl', 			
        database='integrate', 			
        schema='insurance',
        grants = {'ownership': ['FH_READER']},			
    )			
}}


SELECT
  CAST(agentcode AS VARCHAR) AS agentcode,
  AGENTNAME,
  CAST (Null as varchar) as CL_Advisor_Group_Identifier, 
  AGENTSTATUS,
  agenttype,
  brokerid,
  USERDEFINED2,
  PARENTNODEID,
  NODEID,
  NODENAME,
  hierarchypath,
  HIERARCHYPATHNAME,
  REGION,
  MARKET,
  LOCATION,
  FIRM
FROM {{ ref('hierarchy_fh_integrate_insurance') }}

UNION

SELECT
  CAST(ADVISOR_AGREEMENT_NUMBER AS VARCHAR) AS agentcode, 
  ADVISOR_PRODUCER_NAME AS AGENTNAME, 
  ADVISOR_AGREEMENT_GROUP_IDENTIFIER as CL_Advisor_Group_Identifier ,
  CAST(NULL AS VARCHAR) AS AGENTSTATUS, 
  CAST(NULL AS VARCHAR) AS agenttype,
  CAST(NULL AS VARCHAR) AS brokerid,
  CAST(RIGHT(FINANCIAL_HORIZONS_GROUP_IDENTIFIER,9) AS VARCHAR) AS USERDEFINED2,
  CAST(NULL AS VARCHAR) AS PARENTNODEID,
  CAST(NULL AS VARCHAR) AS NODEID,
  CAST(NULL AS VARCHAR) AS NODENAME,
  CAST(NULL AS VARCHAR) AS hierarchypath,
  CAST(NULL AS VARCHAR) AS HIERARCHYPATHNAME,  
  CASE 
    WHEN AC_REGION  = 'QUEBEC' THEN 'Quebec'
    WHEN AC_REGION  = 'ONTARIO (NON-GTA) AND ATLANTIC' THEN 'Ontario Non GTA and Atlantic'
    WHEN AC_REGION  = 'ONTARIO (GTA) AND CAM' THEN 'Ontario GTA and CAM'
    WHEN AC_REGION  = 'WEST' THEN 'West'
    WHEN AC_REGION  = 'MISC REGION' THEN 'Misc Region'
    ELSE AC_REGION 
  END as REGION,
CASE 
    WHEN AC_MARKET  = 'ATLANTIC' THEN 'Atlantic MKT'
    WHEN AC_MARKET  = 'ONTARIO NON-GTA' THEN 'Ontario non-GTA MKT'
    WHEN AC_MARKET  = 'QUEBEC' THEN 'Quebec MKT'
    WHEN AC_MARKET  = 'BRITISH COLUMBIA' THEN 'BC MKT'
    WHEN AC_MARKET  = 'CANADIAN ASIAN MARKETS' THEN 'CAM MKT'
    WHEN AC_MARKET  = 'ALBERTA' THEN 'Alberta MKT'
    WHEN AC_MARKET  = 'GREATER TORONTO AREA' THEN 'GTA MKT'
    WHEN AC_MARKET  = 'PRAIRIES' THEN 'Prairies MKT'
    WHEN AC_MARKET  = 'MISC REGION' THEN 'Misc MKT'
    ELSE AC_MARKET 
  END as MARKET,   
  AC_LOCATION AS LOCATION,
  ADVISOR_REPORTING_FIRM_NAME AS FIRM  
FROM {{ source('acdirect', 'daily_insurance_ac_direct_agreement') }} 



