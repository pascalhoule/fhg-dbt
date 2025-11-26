{{ config (			
        materialized="view",			
        alias='BrokerPhone', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT 
CAST(agentcode AS VARCHAR) AS agentcode,
TYPE,
number
FROM
    {{ ref('brokerphone_vc_report_insurance') }}

UNION

SELECT
CAST(agentcode AS VARCHAR) AS agentcode, 
TELEPHONETYPE AS TYPE,
PHONENUMBER AS number
from
    {{ source('ac_direct_current', 'acdirect_info_current') }} 
