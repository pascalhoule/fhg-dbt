{% snapshot settlementdate %}

{{
    config(
      target_database='NORMALIZE',
      target_schema='snapshots',
      unique_key='policycode',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

SELECT 
    POLICYCODE,
    POLICYGROUPCODE,
    POLICYNUMBER,
    SETTLEMENTDATE,
    UPDATED_AT

FROM {{ ref('policy_vc_clean_insurance') }}

{% endsnapshot %}