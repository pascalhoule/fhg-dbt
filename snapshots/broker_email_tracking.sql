{% snapshot broker_email_tracking %}

{{
    config(
      target_database='applications',
      target_schema='snapshots',
      unique_key='agentcode || type',
      strategy='check',
      check_cols=['EMAILADDRESS'],
    )
}}

SELECT DISTINCT
    AGENTCODE,
	TYPE,
	EMAILADDRESS,
	CASLAPPROVED
FROM {{ ref('brokeremail_vc_integrate_insurance') }}

{% endsnapshot %}