{{  config(alias='sgf_segbudget_fin_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('segbudget_fin_report_segfund') }}