{% snapshot broker_businessemail_tracking %}

{{
    config(
      target_database='applications',
      target_schema='snapshots',
      unique_key='agentcode',
      strategy='check',
      check_cols=['BUSINESS_EMAILADDRESS'],
    )
}}

SELECT DISTINCT
    AGENTCODE,
	BROKERID,
	BUSINESS_EMAILADDRESS,
	PERSON1_EMAILADDRESS,
	PERSON2_EMAILADDRESS,
	AGENTSTATUS 
FROM {{ ref('brokeremailtracking_applications_emailtracking') }}

{% endsnapshot %}