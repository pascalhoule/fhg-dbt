{{  config(alias='sgf_day_seg_production_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('day_seg_production_report_segfund') }}