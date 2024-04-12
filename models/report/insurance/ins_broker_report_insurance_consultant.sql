 {{  config(alias='ins_broker', database='report', schema='insurance')  }} 


SELECT
BROKERID as "BROKER ID",
FIRSTNAME as "FIRST NAME",
LASTNAME as "LAST NAME",
SIN as "SIN",
AGENTCODE as "AGENT CODE",
COMPANYNAME as "COMPANY NAME",
HIERARCHY as "HIERARCHY",
DOB as "DOB",
BUS_EMAIL as "BUS EMAIL",
AGENTTYPE as "AGENT TYPE",
TAGS as "TAGS",
AGENTSTATUS as "AGEN TSTATUS",
BUS_ADDRESS as "BUS ADDRESS",
UD2 as "UD2",
BUS_PHONE as "BUS PHONE",
IS_PRIM_REP as "IS PRIM REP"


from {{ ref ('ins_broker_analyze_insurance')  }}