{{  config(alias='sgf_aua_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('sgf_aua_fh_for_share_clean') }}