{% snapshot maileddate %}

{{
    config(
      target_database='NORMALIZE',
      target_schema='snapshots',
      unique_key='policycode',
      strategy='check',
      check_cols=['MAILEDDATE'],
    )
}}

SELECT DISTINCT
    POLICYCODE,
    POLICYGROUPCODE,
    POLICYNUMBER,
    MAILEDDATE,
    UPDATED_AT
FROM {{ ref('policy_vc_clean_insurance') }}

{% endsnapshot %}