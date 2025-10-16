 {{  config(alias='__base_trans_remove_splitcode', 
    database='report_cl', 
    schema='investment')  }} 

    SELECT * FROM {{ ref('__base_trans_remove_splitcode_analyze_investment') }}