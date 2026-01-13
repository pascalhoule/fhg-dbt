{{  config(alias='sgf_mth_seg_production_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('mth_seg_production_report_segfund') }}