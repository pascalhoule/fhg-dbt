{% snapshot senttoicdate %}

{{
    config(
      target_database='NORMALIZE',
      target_schema='snapshots',
      unique_key='policycode',
      strategy='check',
      check_cols=['SENTTOICDATE'],
    )
}}

SELECT DISTINCT
    POLICYCODE,
    POLICYGROUPCODE,
    POLICYNUMBER,
    SENTTOICDATE,
    UPDATED_AT
FROM {{ ref('policy_vc_clean_insurance') }}

{% endsnapshot %}