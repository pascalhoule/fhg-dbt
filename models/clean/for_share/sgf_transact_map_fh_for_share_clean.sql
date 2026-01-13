{{  config(alias='sgf_transact_map_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('transact_map_report_segfund') }}