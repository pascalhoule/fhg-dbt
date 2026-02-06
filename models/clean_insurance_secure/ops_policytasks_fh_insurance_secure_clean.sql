{{  config(alias='ops_policytasks_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ops_policytasks_fh_for_share_clean') }}