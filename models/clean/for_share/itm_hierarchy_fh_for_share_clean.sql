{{  config(alias='itm_hierarchy_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('hierarchy_fh_report_in_the_mill') }}