{{		config (			
        materialized="view",			
        alias='task_fh', 			
        database='report', 			
        schema='ops_taskreport',
        tags="ops_taskreport"			
    )			}}	

SELECT
    *
FROM
    {{ ref('tasks_fh_clean_insurance') }}