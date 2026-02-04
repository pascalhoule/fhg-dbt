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

CONCAT(
    SUBSTR(REGEXP_REPLACE(PHONENUMBER, '[^0-9]', ''), 1, 3), ' ',
    SUBSTR(REGEXP_REPLACE(PHONENUMBER, '[^0-9]', ''), 4, 3), ' ',
    SUBSTR(REGEXP_REPLACE(PHONENUMBER, '[^0-9]', ''), 7, 4)
) AS number

--PHONENUMBER AS number
from
    {{ source('ac_direct_current', 'acdirect_info_current') }} 
