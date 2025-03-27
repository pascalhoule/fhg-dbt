{% snapshot broker_person1email_tracking %}

{{
    config(
      target_database='applications',
      target_schema='snapshots',
      unique_key='agentcode',
      strategy='check',
      check_cols=['PERSON1_EMAILADDRESS'],
    )
}}

SELECT DISTINCT
    AGENTCODE,
	BROKERID,
	PERSON1_EMAILADDRESS,
	AGENTSTATUS 
FROM {{ ref('brokeremailtracking_applications_emailtracking') }}

{% endsnapshot %}