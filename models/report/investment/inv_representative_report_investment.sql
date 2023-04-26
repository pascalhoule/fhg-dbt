 {{  config(alias='inv_representative', database='report', schema='investment')  }} 


SELECT 
BRANCH_CODE as "BRANCH CODE",
BUS_EMAIL as "BUS EMAIL",
FIRST_NAME as "FIRST NAME",
LAST_NAME as "LAST NAME",
BUS_PHONE as "BUS PHONE",
INSAGENTCODE as "INS AGENT CODE",
REPRESENTIATIVECODE as "REPRESENTIATIVE CODE",
REPID as "REP ID",
IS_PRIM_REP as "IS PRIM REP",
REPSTATUS as "REP STATUS",
BUS_ADDRESS as "BUS ADDRESS",
SIN as "SIN",
DOB as "DOB",
UD2 as "UD2"

from {{ ref ('inv_representative_analyze_investment')  }} rep
