{{ config(alias = 'itm_calcs_FH', 
    materialized = 'view',
    database = 'report', 
    schema = 'insurance') }} 

    SELECT 
        FH_POLICYCATEGORY,
        POLICYNUMBER,
        POLICYCODE,
        CAST(FH_SERVICINGAGTCODE AS VARCHAR(50)) AS FH_SERVICINGAGTCODE,
        FH_STATUSCODE,
        FH_STATUSNAMEENG,
        FH_STATUSCATEGORY,
        FH_STARTDATE,
        CREATEDDATE,
        CONTRACTDATE,
        SETTLEMENTDATE,
        EXPIRYDATE,
        RENEWALDATE,
        SENTTOICDATE,
        MAILEDDATE,
        FH_FINPOSTDATE,
        ITM_END_DATE,
        ITM,
        DAYS_IN_STATUS
    from {{ ref('itm_calcs_FH_analyze_insurance') }}

   