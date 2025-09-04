{{  config(alias='rls_users_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('rls_users_fh_for_share_clean') }}