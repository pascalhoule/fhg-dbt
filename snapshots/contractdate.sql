{% snapshot contractdate %}

{{
    config(
      target_database='NORMALIZE',
      target_schema='snapshots',
      unique_key='policycode',
      strategy='check',
      check_cols=['CONTRACTDATE'],
    )
}}

SELECT DISTINCT
    POLICYCODE,
    POLICYGROUPCODE,
    POLICYNUMBER,
    CONTRACTDATE,
    UPDATED_AT
FROM {{ ref('policy_vc_clean_insurance') }}

{% endsnapshot %}