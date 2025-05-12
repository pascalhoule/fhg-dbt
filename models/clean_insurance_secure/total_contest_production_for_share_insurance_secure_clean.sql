{{  config(alias='total_contest_production', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM
{{ ref('total_contest_production_for_share_clean') }}