 {{ config(alias='employee', database='report', schema='insurance') }}

SELECT 
EMPLOYEECODE as "Employee Code",
FIRSTNAME as "First Name",
LASTNAME as "Last Name",
FULLNAME as "Full Name",
STATUS as "Status",
REGION as "Region"

from {{ ref ('employee_analyze_insurance')  }}