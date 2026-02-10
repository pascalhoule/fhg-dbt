{{  config(alias='ops_policy_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('policy_fh_ops_taskreport') }}