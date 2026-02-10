{{  config(alias='ops_policytasks_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('policytasks_ops_taskreport') }}