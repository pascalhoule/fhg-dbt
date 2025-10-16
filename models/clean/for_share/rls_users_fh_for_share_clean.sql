{{  config(alias='rls_users_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ source('rls_users', 'users') }}