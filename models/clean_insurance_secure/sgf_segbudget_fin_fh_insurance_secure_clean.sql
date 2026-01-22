{{  config(alias='sgf_segbudget_fin_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('sgf_segbudget_fin_fh_for_share_clean') }}