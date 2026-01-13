{{  config(alias='sgf_transact_map_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('sgf_transact_map_fh_for_share_clean') }}