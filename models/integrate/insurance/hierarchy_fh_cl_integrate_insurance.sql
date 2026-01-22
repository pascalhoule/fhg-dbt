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
  CAST(agentcode AS VARCHAR) AS agentcode, 
  AGENTNAME AS AGENTNAME, 
  CL_Advisor_Group_Identifier as CL_Advisor_Group_Identifier ,
  CAST(NULL AS VARCHAR) AS AGENTSTATUS, 
  CAST(NULL AS VARCHAR) AS agenttype,
  CAST(NULL AS VARCHAR) AS brokerid,
  CAST(RIGHT(USERDEFINED2,9) AS VARCHAR) AS USERDEFINED2,
  CAST(NULL AS VARCHAR) AS PARENTNODEID,
  CAST(NULL AS VARCHAR) AS NODEID,
  CAST(NULL AS VARCHAR) AS NODENAME,
  CAST(NULL AS VARCHAR) AS hierarchypath,
  CAST(NULL AS VARCHAR) AS HIERARCHYPATHNAME,  
  CASE 
    WHEN REGION  = 'QUEBEC' THEN 'Quebec'
    WHEN REGION  = 'ONTARIO (NON-GTA) AND ATLANTIC' THEN 'Ontario Non GTA and Atlantic'
    WHEN REGION  = 'ONTARIO (GTA) AND CAM' THEN 'Ontario GTA and CAM'
    WHEN REGION  = 'WEST' THEN 'West'
    WHEN REGION  = 'MISC REGION' THEN 'Misc Region'
    ELSE REGION 
  END as REGION,
CASE 
    WHEN MARKET  = 'ATLANTIC' THEN 'Atlantic MKT'
    WHEN MARKET  = 'ONTARIO NON-GTA' THEN 'Ontario non-GTA MKT'
    WHEN MARKET  = 'QUEBEC' THEN 'Quebec MKT'
    WHEN MARKET  = 'BRITISH COLUMBIA' THEN 'BC MKT'
    WHEN MARKET  = 'CANADIAN ASIAN MARKETS' THEN 'CAM MKT'
    WHEN MARKET  = 'ALBERTA' THEN 'Alberta MKT'
    WHEN MARKET  = 'GREATER TORONTO AREA' THEN 'GTA MKT'
    WHEN MARKET  = 'PRAIRIES' THEN 'Prairies MKT'
    WHEN MARKET  = 'MISC REGION' THEN 'Misc MKT'
    ELSE MARKET 
  END as MARKET,   
  LOCATION AS LOCATION,
  FIRM AS FIRM  
FROM {{ source('ac_direct_current', 'acdirect_info_current') }}  



