{{  config(alias='history_brokeremailtracking', 
    database='applications', 
    schema='Emailtracking',
    materialized = "table",
    grants = {'ownership': ['FH_READER']})  }} 

WITH
 
   BUS_TRACK AS
   (SELECT AGENTCODE, COUNT(*) AS REC_COUNT
   FROM {{ ref('broker_businessemail_tracking') }} 
   GROUP BY 1
   HAVING REC_COUNT > 1),
 
   PER1_TRACK AS
   (SELECT AGENTCODE, COUNT(*) AS REC_COUNT
   FROM {{ ref('broker_person1email_tracking') }}
   GROUP BY 1
   HAVING REC_COUNT > 1
   ),
 
   PER2_TRACK AS
   (SELECT AGENTCODE, COUNT(*) AS REC_COUNT
   FROM {{ ref('broker_person2email_tracking') }} 
   GROUP BY 1
   HAVING REC_COUNT > 1
   )


SELECT
    T.AGENTCODE,
    T.BROKERID,
    T.BUSINESS_EMAILADDRESS,
    Null as PERSON1_EMAILADDRESS,
    Null as PERSON2_EMAILADDRESS,
    T.AGENTSTATUS, --this is the agent status at the time the e-mail changed.
    dbt_valid_from as Email_Changed
  FROM {{ ref('broker_businessemail_tracking') }}  T join BUS_TRACK on BUS_TRACK.agentcode = T.agentcode
   WHERE dbt_valid_to is  not null


UNION 

SELECT
    T.AGENTCODE,
    T.BROKERID,
    Null as BUSINESS_EMAILADDRESS,
    PERSON1_EMAILADDRESS,
    Null as PERSON2_EMAILADDRESS,
    T.AGENTSTATUS, --this is the agent status at the time the e-mail changed.
    dbt_valid_from as Email_Changed
   FROM {{ ref('broker_person1email_tracking') }}  T join PER1_TRACK on PER1_TRACK.agentcode = T.agentcode
    WHERE dbt_valid_to is not null
 
UNION 

  SELECT
    T.AGENTCODE,
    T.BROKERID,
    Null as BUSINESS_EMAILADDRESS,
    Null as PERSON1_EMAILADDRESS,
    PERSON2_EMAILADDRESS,
    T.AGENTSTATUS, --this is the agent status at the time the e-mail changed.
    dbt_valid_from as Email_Changed
   FROM {{ ref('broker_person2email_tracking') }}  T join PER2_TRACK on PER2_TRACK.AGENTCODE = T.AGENTCODE
   WHERE dbt_valid_to is not null
