 {{ config(alias='employee', database='integrate', schema='insurance') }}

select top 10
employee_vc.employeecode,
employee_vc.firstname,
employee_vc.lastname,
concat(employee_vc.firstname, ' ' , employee_vc.lastname) as FullName,
employee_vc.status,
hierarchy_vc.nodename as Region
from {{ ref ('employee_vc_normalize_insurance') }} employee_vc 
left join {{ ref ('hierarchy_vc_normalize_insurance') }} hierarchy_vc on hierarchy_vc.nodeid = employee_vc.parentnodeid

