{{ config (			
        materialized="view",			
        alias='BrokerAddress', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT 
CAST(agentcode AS VARCHAR) AS agentcode,
TYPE, 
ADDRESS, 
CITY, 
PROVINCE, 
POSTAL_CODE, 
COUNTRY
FROM
    {{ ref('brokeraddress_vc_report_insurance') }}

UNION

SELECT
CAST(agentcode AS VARCHAR) AS agentcode, 
TELEPHONETYPE AS TYPE,
CAST('-' AS VARCHAR) AS ADDRESS,
CITY AS CITY,
PROVINCE AS PROVINCE,
POSTALCODE AS POSTAL_CODE,
CAST('-' AS VARCHAR) AS COUNTRY
from
    {{ source('ac_direct_current', 'acdirect_info_current') }} 

