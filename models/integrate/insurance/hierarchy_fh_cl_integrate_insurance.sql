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
  CAST(NULL AS VARCHAR) AS agentcode, 
  ADVISOR_PRODUCER_NAME AS AGENTNAME, 
  CAST(NULL AS VARCHAR) AS AGENTSTATUS, 
  CAST(NULL AS VARCHAR) AS agenttype,
  CAST(NULL AS VARCHAR) AS brokerid,
  CAST(FINANCIAL_HORIZONS_GROUP_IDENTIFIER AS VARCHAR) AS USERDEFINED2,
  CAST(NULL AS VARCHAR) AS PARENTNODEID,
  CAST(NULL AS VARCHAR) AS NODEID,
  CAST(NULL AS VARCHAR) AS NODENAME,
  CAST(NULL AS VARCHAR) AS hierarchypath,
  CAST(NULL AS VARCHAR) AS HIERARCHYPATHNAME,
  AC_REGION AS REGION,
  AC_LOCATION AS LOCATION,
  AC_MARKET AS MARKET,    
  ADVISOR_REPORTING_FIRM_NAME AS FIRM  
FROM {{ source('acdirect', 'daily_insurance_acdirect') }} 

UNION ALL

SELECT
  agentcode,
  AGENTNAME,
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
  LOCATION,
  MARKET,
  FIRM
FROM {{ ref('hierarchy_fh_integrate_insurance') }}