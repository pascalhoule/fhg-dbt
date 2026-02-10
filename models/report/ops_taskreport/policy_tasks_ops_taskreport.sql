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
        assignedby,
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
),

em as (

select 
    usercode,
    trim(concat_ws(' ', firstname, lastname)) as AssignedTo
from {{ ref('employee_vc_clean_insurance') }}

)

select
    tf.taskcode,
    tf.assigndate,
    tf.assignedby,
    tf.title,
    pt.tasktype,
    pt.taskpriority,
    pt.taskstatus,
    pt.startdate,
    pt.usercode,
    pt.duedate,
    pt.completiondate,
    pt.policycode,
    em.AssignedTo
from tf
-- Use LEFT JOIN to keep all OPS tasks (recommended for reporting completeness)
left join pt
    on tf.taskcode = pt.taskcode
left join em
    on pt.usercode = em.usercode

-- If you only want matched rows, replace the LEFT JOIN with:
-- inner join pt on tf.taskcode = pt.taskcode
