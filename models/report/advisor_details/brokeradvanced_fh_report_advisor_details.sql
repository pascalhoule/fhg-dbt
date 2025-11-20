{{ config (			
        materialized="view",			
        alias='Brokeradvanced', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	

SELECT
  CAST(agentcode AS VARCHAR) AS agentcode,
  CAST (Null as varchar) as CL_Advisor_Group_Identifier,
  USERDEFINED2,
  USERDEFINED1,
  CAST (Null as varchar) as SuperUID,

FROM
       {{ ref('brokeradvanced_vc_report_insurance') }} 

UNION

SELECT
  CAST(agentstatus AS VARCHAR) AS agentcode, 
  CL_Advisor_Group_Identifier ,
  CAST(USERDEFINED2 AS VARCHAR) AS USERDEFINED2,
  CAST(NULL AS VARCHAR) AS USERDEFINED1,
  COALESCE(
    NULLIF(USERDEFINED2, '-'),CL_Advisor_Group_Identifier) AS SuperUID
from
    {{ source('ac_direct_current', 'acdirect_info_current') }} 
