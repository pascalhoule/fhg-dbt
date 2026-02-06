{{  config(alias='ops_tasks1_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ops_tasks1_fh_for_share_clean') }}