{{  config(alias='sgf_mth_seg_production_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('sgf_mth_seg_production_fh_for_share_clean') }}