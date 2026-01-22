{{  config(alias='sgf_aua_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('aua_report_segfund') }}