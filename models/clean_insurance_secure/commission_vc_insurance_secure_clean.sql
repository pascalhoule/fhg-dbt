{{  config(alias='commission_vc', 
database='clean', 
schema='insurance_secure')  }} 

SELECT
    PAIDDATE, 
    COMMISSIONPAIDDATE, 
    ACCRUALPAIDDATE, 
    PAIDBY, 
    NODEID, 
    OWNERCODE, 
    ICNAME, 
    PLANTYPE, 
    PLANNAME, 
    TRXTYPE, 
    COMMISSIONAMOUNT, 
    {{ DATABRICKS_MASK('POLICYCODE') }} as policycode ,
    COMMISSIONSTATUS, 
    ACCRUALSTATUS
FROM {{ ref('commission_vc_clean_insurance') }}
WHERE ICNAME = 'Canada Life / Canada-Vie'