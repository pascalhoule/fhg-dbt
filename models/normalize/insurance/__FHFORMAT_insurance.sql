{{  config(alias='__FHFORMAT', database='normalize', schema='insurance')  }} 

 SELECT
        POLICYCATEGORY,
        POLICYCODE,
        POLICYGROUPCODE,
        SERVICINGAGTCODE,
        SPLIT_PART(CARRIER, ' / ', 1) AS CARRIERENG,
        SPLIT_PART(CARRIER, ' / ', 2) AS CARRIERFR,
        POLICYNUMBER,
        PLANID,
        CASE
            WHEN PLANTYPE = 'HealthSickness' THEN 'H&S'
            WHEN PLANTYPE = 'SERVICE' THEN 'Service'
            ELSE PLANTYPE
        END AS PLANTYPE,
        SPLIT_PART(PLANNAME, '/', 1) AS PLANNAMEENG,
        SPLIT_PART(PLANNAME, '/', 2) AS PLANNAMEFR,
        PREMIUMAMOUNT,
        ANNUALPREMIUMAMOUNT,
        COMMPREMIUMAMOUNT,
        PAYMENTMODE,
        FACEAMOUNT,
        CREATEDBY,
        CREATEDDATE,
        APPLICATIONDATE,
        CONTRACTDATE,
        SETTLEMENTDATE,
        EXPIRYDATE,
        RENEWALDATE,
        SENTTOICDATE,
        MAILEDDATE,
        FINPOSTDATE,
        STATUS AS STATUSCODE,
        STATUS.ENGLISHDESCRIPTION AS STATUSNAMEENG,
        STATUS.FRENCHDESCRIPTION AS STATUSNAMEFR,
        STATUS.CATEGORY AS STATUSCATEGORY,
        SERVICINGAGTSPLIT,
        APPCOUNT,
        FYCAMOUNT,
        MGAFYOAMOUNT,
        ISSUEPROVINCE,
        CASE
            WHEN APPSOURCE = 'manual' THEN 'Manual'
            WHEN APPSOURCE = 'feed' THEN 'Feed'
            ELSE APPSOURCE
        END AS APPSOURCE,
        CASE
            WHEN APPTYPE = 'paper' THEN 'Paper'
            WHEN APPTYPE = 'eapp' THEN 'Eapp'
            ELSE APPTYPE
        END AS APPTYPE,
        LASTCOMMISSIONPROCESSDATE,
        FIRSTOWNERCLIENTCODE,
        FIRSTINSUREDCLIENTCODE,
        ISMAINCOVERAGE,
        FH_SETTLEMENTDATE,
        LEAST(APPLICATIONDATE, CREATEDDATE) AS FH_STARTDATE,
        CASE
            WHEN COMMPREMIUMAMOUNT is not null THEN IFF(
                COMMPREMIUMAMOUNT = 0,
                ANNUALPREMIUMAMOUNT,
                COMMPREMIUMAMOUNT
            )
            WHEN COMMPREMIUMAMOUNT is null THEN 0
        END AS FH_PREMIUM
    FROM
        {{ ref ('__POSTDATE_insurance') }} POL
        JOIN {{ ref ('policystatus_vc_normalize_insurance') }} STATUS ON POL.STATUS = STATUS.CODE