 {{ config(alias='employee', database='analyze', schema='insurance') }}

select 
*

from {{ ref ('employee_integrate_insurance') }} c
