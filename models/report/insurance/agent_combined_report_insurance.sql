 {{  config(alias='agent_combined', database='report', schema='insurance')  }} 


SELECT 
INS_AGENTCODE as "INS AGENTCODE",
PERSON_UID as "PERSON UID",
BUS_ADDRESS as "BUS ADDRESS",
DOB as "DOB",
SIN as "SIN",
UD2 as "UD2",
BUS_EMAIL as "BUS EMAIL",
FIRSTNAME as "FIRSTNAME",
BUS_PHONE as "BUS PHONE",
IS_PRIM_REP as "IS PRIM REP",
LASTNAME as "LASTNAME"



from {{ ref ('agent_combined_analyze_insurance')  }}