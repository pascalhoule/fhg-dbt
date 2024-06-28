{% snapshot status %}

{{
    config(
      target_database='NORMALIZE',
      target_schema='snapshots',
      unique_key='policycode',
      strategy='check',
      check_cols = ['STATUS'],
    )
}}

SELECT DISTINCT
    POLICYCODE,
    POLICYGROUPCODE,
    POLICYNUMBER,
    STATUS,
    UPDATED_AT
FROM {{ ref('policy_vc_clean_insurance') }}

{% endsnapshot %}