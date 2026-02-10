{{		config (			
        materialized="view",			
        alias='policy_tasks', 			
        database='report', 			
        schema='ops_taskreport',
        tags="ops_taskreport"			
    )			}}	

with tf as (
    -- OPS tasks (secure)
    select
        taskcode,
        assigndate,
        assignby,
        title
    from {{ ref('task_fh_ops_taskreport') }}
),
pt as (
    -- Policy task details (non-secure)
    select
        taskcode,
        tasktype,
        taskpriority,
        taskstatus,
        startdate,
        usercode,
        duedate,
        completiondate,
        policycode
    from {{ ref('policytasks_ops_taskreport') }}
)

select
    tf.taskcode,
    tf.assigndate,
    tf.assignby,
    tf.title,
    pt.tasktype,
    pt.taskpriority,
    pt.taskstatus,
    pt.startdate,
    pt.usercode,
    pt.duedate,
    pt.completiondate,
    pt.policycode
from tf
-- Use LEFT JOIN to keep all OPS tasks (recommended for reporting completeness)
left join pt
    on tf.taskcode = pt.taskcode

-- If you only want matched rows, replace the LEFT JOIN with:
-- inner join pt on tf.taskcode = pt.taskcode
