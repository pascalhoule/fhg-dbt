{% snapshot settlementdate %}

{{
    config(
      target_database='NORMALIZE',
      target_schema='snapshots',
      unique_key='policycode',
      strategy='check',
      check_cols=['SETTLEMENTDATE'],
    )
}}

SELECT DISTINCT
    POLICYCODE,
    POLICYGROUPCODE,
    POLICYNUMBER,
    SETTLEMENTDATE,
    UPDATED_AT
FROM {{ ref('policy_vc_clean_insurance') }}

{% endsnapshot %}