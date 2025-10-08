 {{  config(alias='__base_trans_remove_splitcode', 
    database='analyze', 
    schema='investment')  }} 



SELECT *
FROM {{ ref('__base_trans_remove_splitcode_normalize_investment') }}