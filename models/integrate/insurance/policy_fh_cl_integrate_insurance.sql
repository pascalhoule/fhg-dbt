{{
    config(
        materialized="view",
        alias="policy_fh_cl",
        database="integrate",
        schema="insurance" 
    )
}}

WITH

U_CODE_MAPPING AS (
    SELECT DISTINCT
        B.CL_ADVISOR_GROUP_IDENTIFIER,
        B.USERDEFINED2,
        B.BROKERID,
        B.SEGMENTTAGWS AS SEGMENT,
        B.COS_SALES_BDC,
        B.COS_SALES_BDD,
        B.COS_SALES_RVP,
        H.REGION,
        H.MARKET,
        H.LOCATION
    FROM
        {{ ref('broker_fh_cl_integrate_insurance') }} AS B
    INNER JOIN {{ ref('hierarchy_fh_cl_integrate_insurance') }} AS H
        ON
            B.AGENTCODE = H.AGENTCODE
            AND B.CL_ADVISOR_GROUP_IDENTIFIER IS NOT null
),

POL_WITH_UD2 AS (
    SELECT
        POL_BASE.*,
        COALESCE(B_SERV.USERDEFINED2, B_SERV_UCODE.USERDEFINED2)
            AS FH_SERVICINGAGT_UD2,
        COALESCE(B_COMM.USERDEFINED2, B_COMM_UCODE.USERDEFINED2)
            AS FH_COMMISSIONINGAGT_UD2
    FROM
        INTEGRATE.PROD_INSURANCE.__BASE_POL_FH_CL AS POL_BASE
    LEFT JOIN
        {{ ref('broker_fh_cl_integrate_insurance') }} AS B_COMM
        ON POL_BASE.FH_COMMISSIONINGAGTCODE = B_COMM.AGENTCODE
    LEFT JOIN
        U_CODE_MAPPING AS B_COMM_UCODE
        ON
            POL_BASE.FH_COMMISSIONINGAGTCODE
            = B_COMM_UCODE.CL_ADVISOR_GROUP_IDENTIFIER
    LEFT JOIN
        {{ ref('broker_fh_cl_integrate_insurance') }} AS B_SERV
        ON POL_BASE.FH_SERVICINGAGTCODE = B_SERV.AGENTCODE
    LEFT JOIN
        U_CODE_MAPPING AS B_SERV_UCODE
        ON
            POL_BASE.FH_SERVICINGAGTCODE
            = B_SERV_UCODE.CL_ADVISOR_GROUP_IDENTIFIER
),

BROKER_ACTIVE AS (
    SELECT DISTINCT
        B.USERDEFINED2,
        B.CL_ADVISOR_GROUP_IDENTIFIER,
        FIRST_VALUE(B.AGENTNAME)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS AGENTNAME,
        FIRST_VALUE(B.AGENTCODE)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS AGENTCODE,
        FIRST_VALUE(B.BROKERID)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS BROKERID,
        FIRST_VALUE(H.REGION)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS REGION,
        FIRST_VALUE(H.MARKET)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS MARKET,
        FIRST_VALUE(H.LOCATION)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS LOCATION,
        FIRST_VALUE(B.COS_SALES_BDC)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_BDC,
        FIRST_VALUE(B.COS_SALES_BDD)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_BDD,
        FIRST_VALUE(B.COS_SALES_RVP)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_RVP,
        FIRST_VALUE(B.SEGMENTTAGWS)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS SEGMENTTAGWS
    FROM {{ ref('broker_fh_cl_integrate_insurance') }} AS B
    LEFT JOIN
        {{ ref('hierarchy_fh_cl_integrate_insurance') }} AS H
        ON B.AGENTCODE = H.AGENTCODE
    WHERE B.AGENTSTATUS = 'Active'
),

REST_OF_BROKER_LIST AS (
    SELECT DISTINCT
        B.USERDEFINED2,
        B.CL_ADVISOR_GROUP_IDENTIFIER,
        FIRST_VALUE(B.AGENTNAME)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS AGENTNAME,
        FIRST_VALUE(B.AGENTCODE)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS AGENTCODE,
        FIRST_VALUE(B.BROKERID)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS BROKERID,
        FIRST_VALUE(H.REGION)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS REGION,
        FIRST_VALUE(H.MARKET)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS MARKET,
        FIRST_VALUE(H.LOCATION)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS, B.CREATEDDATE
            )
            AS LOCATION,
        FIRST_VALUE(B.COS_SALES_BDC)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_BDC,
        FIRST_VALUE(B.COS_SALES_BDD)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_BDD,
        FIRST_VALUE(B.COS_SALES_RVP)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS COS_SALES_RVP,
        FIRST_VALUE(B.SEGMENTTAGWS)
            OVER (
                PARTITION BY
                    B.USERDEFINED2,
                    B.CL_ADVISOR_GROUP_IDENTIFIER,
                    B.AGENTNAME,
                    B.AGENTCODE
                ORDER BY B.AGENTSTATUS ASC, B.CREATEDDATE DESC
            )
            AS SEGMENTTAGWS
    FROM {{ ref('broker_fh_cl_integrate_insurance') }} AS B
    INNER JOIN
        {{ ref('hierarchy_fh_cl_integrate_insurance') }} AS H
        ON B.AGENTCODE = H.AGENTCODE
    WHERE B.AGENTCODE NOT IN (SELECT B.AGENTCODE FROM BROKER_ACTIVE AS B)
)

SELECT
    POL_WITH_UD2.*,
    H_COMM.USERDEFINED2 AS FH_COMMISSIONINGAGT_USERDEFINED2,
    H_SERV.USERDEFINED2 AS FH_SERVICINGAGT_USERDEFINED2,
    INITCAP(H_COMM.REGION) AS FH_COMMISSIONINGAGT_REGION,
    INITCAP(H_COMM.MARKET) AS FH_COMMISSIONINGAGT_MARKET,
    INITCAP(H_COMM.LOCATION) AS FH_COMMISSIONINGAGT_LOCATION,
    INITCAP(H_SERV.REGION) AS FH_SERVICINGAGT_REGION,
    INITCAP(H_SERV.MARKET) AS FH_SERVICINGAGT_MARKET,
    INITCAP(H_SERV.LOCATION) AS FH_SERVICINGAGT_LOCATION,
    COALESCE(B_COMM.BROKERID, B_COMM_INACTIVE.BROKERID)
        AS FH_COMMISSIONINGAGT_BROKERID,
    COALESCE(B_COMM.SEGMENTTAGWS, B_COMM_INACTIVE.SEGMENTTAGWS)
        AS FH_COMMISSIONINGAGT_SEGMENTTAGWS,
    COALESCE(B_COMM.COS_SALES_BDC, B_COMM_INACTIVE.COS_SALES_BDC)
        AS FH_COMMISSIONINGAGT_COS_SALES_BDC,
    COALESCE(B_COMM.COS_SALES_BDD, B_COMM_INACTIVE.COS_SALES_BDD)
        AS FH_COMMISSIONINGAGT_COS_SALES_BDD,
    COALESCE(B_COMM.COS_SALES_RVP, B_COMM_INACTIVE.COS_SALES_RVP)
        AS FH_COMMISSIONINGAGT_COS_SALES_RVP,
    INITCAP(
        COALESCE(
            B_COMM.AGENTNAME,
            B_COMM_INACTIVE.AGENTNAME,
            H_COMM.AGENTNAME,
            H_COMM.AGENTNAME
        )
    )
        AS FH_COMMISSIONINGAGT_NAME,
    COALESCE(B_SERV.BROKERID, B_SERV_INACTIVE.BROKERID)
        AS FH_SERVICINGAGT_BROKERID,
    COALESCE(B_SERV.SEGMENTTAGWS, B_SERV_INACTIVE.SEGMENTTAGWS)
        AS FH_SERVICINGAGT_SEGMENTTAGWS,
    COALESCE(B_SERV.COS_SALES_BDC, B_SERV_INACTIVE.COS_SALES_BDC)
        AS FH_SERVICINGAGT_COS_SALES_BDC,
    COALESCE(B_SERV.COS_SALES_BDD, B_SERV_INACTIVE.COS_SALES_BDD)
        AS FH_SERVICINGAGT_COS_SALES_BDD,
    COALESCE(B_SERV.COS_SALES_RVP, B_SERV_INACTIVE.COS_SALES_RVP)
        AS FH_SERVICINGAGT_COS_SALES_RVP,
    INITCAP(
        COALESCE(B_SERV.AGENTNAME, B_SERV_INACTIVE.AGENTNAME, H_SERV.AGENTNAME)
    )
        AS FH_SERVICINGAGT_NAME

FROM POL_WITH_UD2
LEFT JOIN {{ ref('hierarchy_fh_cl_integrate_insurance') }} AS H_COMM
    ON POL_WITH_UD2.FH_COMMISSIONINGAGTCODE = H_COMM.AGENTCODE
LEFT JOIN BROKER_ACTIVE AS B_COMM
    ON
        POL_WITH_UD2.FH_COMMISSIONINGAGT_UD2 = B_COMM.USERDEFINED2
        AND POL_WITH_UD2.FH_COMMISSIONINGAGTCODE = B_COMM.AGENTCODE
LEFT JOIN REST_OF_BROKER_LIST AS B_COMM_INACTIVE
    ON POL_WITH_UD2.FH_COMMISSIONINGAGTCODE = B_COMM_INACTIVE.AGENTCODE
LEFT JOIN {{ ref('hierarchy_fh_cl_integrate_insurance') }} AS H_SERV
    ON POL_WITH_UD2.FH_SERVICINGAGTCODE = H_SERV.AGENTCODE
LEFT JOIN BROKER_ACTIVE AS B_SERV
    ON
        POL_WITH_UD2.FH_SERVICINGAGT_UD2 = B_SERV.USERDEFINED2
        AND POL_WITH_UD2.FH_SERVICINGAGTCODE = B_SERV.AGENTCODE
LEFT JOIN REST_OF_BROKER_LIST AS B_SERV_INACTIVE
    ON POL_WITH_UD2.FH_SERVICINGAGTCODE = B_SERV_INACTIVE.AGENTCODE
